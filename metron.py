"""
Metron.cloud information source for Comic Tagger
"""
# Copyright comictagger team
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
from __future__ import annotations

import argparse
import logging
import pathlib
from typing import Any, Callable

import comictalker.talker_utils as talker_utils
import mokkari
import settngs
from comicapi import utils
from comicapi.genericmetadata import GenericMetadata
from comictalker.comiccacher import ComicCacher
from comictalker.comictalker import ComicTalker, TalkerNetworkError
from comictalker.resulttypes import ComicIssue, ComicSeries, Credit
from mokkari.issue import Issue, IssuesList
from mokkari.series import Series, SeriesList

logger = logging.getLogger(__name__)


class MetronTalkerExt(ComicTalker):
    name: str = "Metron"
    id: str = "metron"
    website: str = "https://metron.cloud"
    logo_url: str = "https://static.metron.cloud/static/site/img/metron.svg"
    attribution: str = f"Metadata provided by <a href='{website}'>{name}</a>"

    def __init__(self, version: str, cache_folder: pathlib.Path):
        super().__init__(version, cache_folder)
        # Default settings
        self.default_api_url = self.api_url = f"{self.website}/api/"
        self.default_api_key = self.api_key = ""
        self.username: str = ""
        self.user_password: str = self.api_key
        self.use_series_start_as_volume: bool = False
        # TODO Probably too heavy on API?
        self.check_variants: bool = False

    def register_settings(self, parser: settngs.Manager) -> None:
        parser.add_setting(
            "--met-use-series-start-as-volume",
            default=False,
            action=argparse.BooleanOptionalAction,
            display_name="Use series start as volume",
            help="Use the series start year as the volume number",
        )
        """parser.add_setting(
            "--met-check-variants",
            default=False,
            action=argparse.BooleanOptionalAction,
            display_name="Check cover variants when auto-tagging",
            help="Check for cover variants. *This will cause an additional API call and may result in longer times*",
        )"""
        parser.add_setting(
            "--met-username",
            default="",
            display_name="Username",
            help="Username for Metron website. (NOTE: Test API requires username to be saved.)",
        )
        parser.add_setting(
            f"--{self.id}-key",
            default="",
            display_name="API Password",
            help=f"Use the given Metron API password. (default: {self.default_api_key})",
        )
        # TODO Hide
        parser.add_setting(
            f"--{self.id}-url",
            default="",
            display_name="API URL",
            help=f"Use the given Metron URL. (default: {self.default_api_url})",
        )

    def parse_settings(self, settings: dict[str, Any]) -> dict[str, Any]:
        settings = super().parse_settings(settings)

        self.username = settings["met_username"]
        self.user_password = settings["metron_key"]

        return settings

    def check_api_key(self, url: str, key: str) -> tuple[str, bool]:
        metron_api = mokkari.api(self.username, key, user_agent="comictagger/" + self.version)
        try:
            metron_api.series(1)
            return "The API access test was successful", True
        except mokkari.exceptions.AuthenticationError:
            return "Access denied. Invalid username or password.", False
        except mokkari.exceptions.ApiError as e:
            return f"API error: {e}", False

    def search_for_series(
        self,
        series_name: str,
        callback: Callable[[int, int], None] | None = None,
        refresh_cache: bool = False,
        literal: bool = False,
        series_match_thresh: int = 90,
    ) -> list[ComicSeries]:
        search_series_name = utils.sanitize_title(series_name, literal)
        logger.info(f"{self.name} searching: {search_series_name}")

        # Before we search online, look in our cache, since we might have done this same search recently
        # For literal searches always retrieve from online
        cvc = ComicCacher(self.cache_folder, self.version)
        if not refresh_cache and not literal:
            cached_search_results = cvc.get_search_results(self.id, series_name)

            if len(cached_search_results) > 0:
                return cached_search_results

        met_response: SeriesList = self._get_metron_content("series_list", {"name": search_series_name})

        # Format result to ComicIssue
        formatted_search_results = self._format_search_results(met_response.series)

        # Cache these search results, even if it's literal we cache the results
        # The most it will cause is extra processing time
        cvc.add_search_results(self.id, series_name, formatted_search_results)

        return formatted_search_results

    def fetch_comic_data(
        self,
        issue_id: str | None = None,
        series_id: str | None = None,
        issue_number: str = "",
    ) -> GenericMetadata:
        comic_data = GenericMetadata()
        if issue_id:
            comic_data = self._fetch_issue_data_by_issue_id(issue_id)
        elif issue_number and series_id:
            comic_data = self._fetch_issue_data(int(series_id), issue_number)

        return comic_data

    def fetch_issues_by_series(self, series_id: int) -> list[ComicIssue]:
        # before we search online, look in our cache, since we might already have this info
        cvc = ComicCacher(self.cache_folder, self.version)
        cached_series_issues_result = cvc.get_series_issues_info(str(series_id), self.id)

        # Need the issue count to check against the cached issue list
        series_data = self._fetch_series_data(series_id)

        # Check cache length against count of issues in case a new issue has
        if len(cached_series_issues_result) == series_data.count_of_issues:
            return cached_series_issues_result

        # TODO If option is set, fetch full issue info for variant covers and/or use variant call when added
        met_response: IssuesList = self._get_metron_content("issues_list", {"series_id": series_id})

        # Format to ComicIssue
        # Pass along series id as issue list won't contain it
        formatted_series_issues_result = self._format_issue_list_results(met_response, series_id)

        cvc.add_series_issues_info(self.id, formatted_series_issues_result)

        return formatted_series_issues_result

    def fetch_issues_by_series_issue_num_and_year(
        self, series_id_list: list[str], issue_number: str, year: str | int | None
    ) -> list[ComicIssue]:
        issues_result = []
        int_year = utils.xlate_int(year)
        if int_year is not None:
            year = str(int_year)

        for series_id in series_id_list:
            params = {
                "series_id": series_id,
                "number": issue_number,
            }

            if int_year:
                params["cover_year"] = year  # type: ignore

            met_response: IssuesList = self._get_metron_content("issues_list", params)
            # Pass along series id as issue list does not contain it
            formatted_result = self._format_issue_list_results(met_response, int(series_id))
            issues_result.extend(formatted_result)

        return issues_result

    def _get_metron_content(
        self, endpoint: str, params: dict[str, Any] | int
    ) -> list[Series] | list[Issue] | Issue | Series | SeriesList | IssuesList:
        """Use the mokkari python library to retrieve data from Metron.cloud"""
        metron_api = mokkari.api(self.username, self.user_password, user_agent="comictagger/" + self.version)

        try:
            result = getattr(metron_api, endpoint)(params)
        except mokkari.exceptions.AuthenticationError:
            logger.debug("Access denied. Invalid username or password.")
            raise TalkerNetworkError(self.name, 1, "Access denied. Invalid username or password.")
        except mokkari.exceptions.ApiError as e:
            logger.debug(f"API error: {e}")
            raise TalkerNetworkError(self.name, 1, f"API error: {e}")

        return result

    def _format_search_results(self, search_results: SeriesList) -> list[ComicSeries]:
        formatted_results = []
        for record in search_results:
            # Flatten publisher to name only
            pub_name = ""
            # pub_name = record.publisher.name

            # Option to use sort name?
            # display_name contains (year) which will mess up fuzzy search results
            series_name = record.display_name
            series_name_array = record.display_name.split("(")
            series_name_len = len(series_name_array)
            if series_name_len:
                series_name = ""
                for i, series in enumerate(series_name_array):
                    if i < series_name_len - 1:
                        series_name += series.replace(")", "")

            img = ""
            # series_name = record.image

            formatted_results.append(
                ComicSeries(
                    aliases=[],
                    count_of_issues=record.issue_count,
                    description="",
                    id=str(record.id),
                    image_url=img,
                    name=series_name,
                    publisher=pub_name,
                    start_year=record.year_began,
                )
            )

        return formatted_results

    def _format_series_results(self, search_result: Series) -> ComicSeries:
        # Flatten publisher to name only
        pub_name = search_result.publisher.name

        # TODO Add additional fields when added to ComicSeries

        # Option to use sort name?
        series_name = search_result.name

        desc = search_result.desc

        img = ""
        # TODO When in API
        # img = record.image

        formatted_result = ComicSeries(
            aliases=[],
            count_of_issues=search_result.issue_count,
            description=desc,
            id=str(search_result.id),
            image_url=img,
            name=series_name,
            publisher=pub_name,
            start_year=search_result.year_began,
        )

        return formatted_result

    def _format_issue_list_results(self, issue_results: IssuesList, series_id: int = 0) -> list[ComicIssue]:
        formatted_results = []
        for record in issue_results:
            if series_id:
                series = self._fetch_series_data(series_id)
            else:
                series = ComicSeries(
                    aliases=[],
                    id="0",
                    count_of_issues=None,
                    description="",
                    image_url="",
                    name=record.series.name,
                    publisher="",
                    start_year=record.series.year_began,
                )

            formatted_results.append(
                ComicIssue(
                    aliases=[],
                    cover_date=str(record.cover_date),
                    description="",
                    id=str(record.id),
                    image_url=record.image,
                    issue_number=record.number,
                    name=record.issue_name,
                    site_detail_url="",
                    series=series,
                    alt_image_urls=[],
                    characters=[],
                    locations=[],
                    teams=[],
                    story_arcs=[],
                    credits=[],
                    complete=False,
                )
            )

        return formatted_results

    def _format_issue_results(self, issue_result: Issue, complete: bool = False, series_id: int = 0) -> ComicIssue:
        # TODO Add genres, age rating, volume, etc. when fields have been added to ComicIssue

        alt_images_list = []
        for variant in issue_result.variants:
            alt_images_list.append(variant.image)

        character_list = []
        for char in issue_result.characters:
            character_list.append(char.name)

        location_list: list = []

        teams_list = []
        for team in issue_result.teams:
            teams_list.append(team.name)

        story_list = []
        for arc in issue_result.arcs:
            story_list.append(arc.name)

        persons_list = []
        for person in issue_result.credits:
            # Creator can have multiple roles
            for role in person.role:
                persons_list.append(Credit(name=person.creator, role=role.name))

        if series_id:
            series = self._fetch_series_data(series_id)
        else:
            series = self._fetch_series_data(issue_result.series.id)

        # series_type == 2 is "cancelled series" there appears to be no "ended"
        # 11 "Limited Series"
        # 5 "One-Shot"
        # 1 "Ongoing Series"
        # TODO When ComicSeries has format, use to validate count

        if issue_result.series.series_type.id == 1:
            series.count_of_issues = None

        name = issue_result.collection_title

        # Add option to use first (all csv?) story title if no title
        # if issue_result.story_titles[0]:
        # name = issue_result.story_titles[0]

        formatted_result = ComicIssue(
            aliases=[],
            cover_date=str(issue_result.cover_date),
            description=issue_result.desc,
            id=str(issue_result.id),
            image_url=issue_result.image,
            issue_number=issue_result.number,
            name=name,
            site_detail_url=issue_result.resource_url,
            series=series,
            alt_image_urls=alt_images_list,
            characters=character_list,
            locations=location_list,
            teams=teams_list,
            story_arcs=story_list,
            credits=persons_list,
            complete=complete,
        )

        return formatted_result

    def _fetch_series_data(self, series_id: int) -> ComicSeries:
        # before we search online, look in our cache, since we might already have this info
        cvc = ComicCacher(self.cache_folder, self.version)
        cached_series_result = cvc.get_series_info(str(series_id), self.id)

        # No publisher (or desc) indicates none full record
        if cached_series_result is not None and cached_series_result.publisher:
            return cached_series_result

        met_response: Series = self._get_metron_content("series", series_id)
        formatted_series_results = self._format_series_results(met_response)

        if met_response:
            cvc.add_series_info(self.id, formatted_series_results)

        return formatted_series_results

    def _fetch_issue_data(self, series_id: int, issue_number: str) -> GenericMetadata:
        met_response: IssuesList = self._get_metron_content(
            "issues_list", {"series_id": series_id, "number": issue_number}
        )
        if len(met_response.issues) > 0:
            # Presume only one result
            return self._fetch_issue_data_by_issue_id(met_response.issues[0].id)

        return GenericMetadata()

    def _fetch_issue_data_by_issue_id(self, issue_id: str) -> GenericMetadata:
        # before we search online, look in our cache, since we might already have this info
        cvc = ComicCacher(self.cache_folder, self.version)
        cached_issues_result = cvc.get_issue_info(int(issue_id), self.id)

        if cached_issues_result and cached_issues_result.complete:
            # TODO Some way to remove count_of_issues if ongoing? Possible when format is in ComicSeries
            return talker_utils.map_comic_issue_to_metadata(
                cached_issues_result,
                self.name,
                False,
                self.use_series_start_as_volume,
            )

        met_response: Issue = self._get_metron_content("issue", int(issue_id))

        # Format to expected output
        met_issue = self._format_issue_results(met_response, True, 0)

        # Get full series info
        series_data = self._fetch_series_data(met_response.series.id)

        # Count of issues is valid if cancelled (ended)
        if met_response.series.series_type == 2:
            met_issue.series.count_of_issues = series_data.count_of_issues
        else:
            met_issue.series.count_of_issues = None

            # Copy desc as it will be missing
        met_issue.series.description = series_data.description

        cvc.add_series_issues_info(self.id, [met_issue])

        # Now, map the ComicIssue data to generic metadata
        return talker_utils.map_comic_issue_to_metadata(
            met_issue,
            self.name,
            False,
            self.use_series_start_as_volume,
        )

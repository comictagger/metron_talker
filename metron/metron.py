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
import re
from typing import Any, Callable

import mokkari
import settngs
from comicapi import utils
from comicapi.genericmetadata import ComicSeries, GenericMetadata, TagOrigin
from comicapi.issuestring import IssueString
from comictalker.comictalker import ComicTalker, TalkerNetworkError
from mokkari.issue import Issue, IssuesList
from mokkari.series import AssociatedSeries, Series, SeriesList

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
        self.display_variants: bool = False
        self.use_ongoing_issue_count: bool = False
        self.find_series_covers: bool = False

    def register_settings(self, parser: settngs.Manager) -> None:
        parser.add_setting(
            "--met-use-series-start-as-volume",
            default=False,
            action=argparse.BooleanOptionalAction,
            display_name="Use series start as volume",
            help="Use the series start year as the volume number",
        )
        # Hide from CLI as it is GUI related
        parser.add_setting(
            "--met-display-variants",
            default=False,
            cmdline=False,
            action=argparse.BooleanOptionalAction,
            display_name="Display variant covers in the issue list",
            help="Make variant covers available in the issue list window.  *May result in longer load times*",
        )
        parser.add_setting(
            "--met-series-covers",
            default=False,
            cmdline=False,
            action=argparse.BooleanOptionalAction,
            display_name="Attempt to fetch a cover for each series",
            help="Fetches a cover for each series in the series selection window. "
            "*This will cause a delay in showing the series window list!*",
        )
        parser.add_setting(
            "--met-use-ongoing",
            default=False,
            action=argparse.BooleanOptionalAction,
            display_name="Use the ongoing issue count",
            help='If a series is labelled as "ongoing", use the current issue count (otherwise empty)',
        )
        parser.add_setting(
            "--met-username",
            default="",
            display_name="Username",
            help="Username for Metron website",
        )
        parser.add_setting(
            f"--{self.id}-key",
            default="",
            display_name="API Password",
            help="Use the given Metron API password",
        )
        parser.add_setting(
            f"--{self.id}-url",
            default="",
            display_name="API URL",
            help=f"Use the given Metron URL. (default: {self.default_api_url})",
        )

    def parse_settings(self, settings: dict[str, Any]) -> dict[str, Any]:
        settings = super().parse_settings(settings)

        self.use_series_start_as_volume = settings["met_use_series_start_as_volume"]
        self.display_variants = settings["met_display_variants"]
        self.find_series_covers = settings["met_series_covers"]
        self.use_ongoing_issue_count = settings["met_use_ongoing"]
        self.username = settings["met_username"]
        self.user_password = settings["metron_key"]

        return settings

    def check_status(self, settings: dict[str, Any]) -> tuple[str, bool]:
        try:
            metron_api = mokkari.api(
                settings["met_username"], settings["metron_key"], user_agent="comictagger/" + self.version
            )
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

        # mokkari is handling caching
        met_response: SeriesList = self._get_metron_content("series_list", {"name": search_series_name})

        formatted_search_results = self._format_search_results(met_response.series)

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

    def fetch_issues_in_series(self, series_id: str) -> list[GenericMetadata]:
        return [x[0] for x in self._fetch_issues_in_series(int(series_id))]

    def _fetch_issues_in_series(self, series_id: int) -> list[tuple[GenericMetadata, bool]]:
        # mokkari handles cache
        met_response: IssuesList = self._get_metron_content("issues_list", {"series_id": series_id})

        # To cause a load for full issue in the issue window, need to remove image if supporting variant covers
        # This should only affect the GUI
        if self.display_variants:
            for issue in met_response:
                issue.image = ""

        # Format to expected output
        formatted_series_issues_result = [
            self._map_comic_issue_to_metadata(x, self._fetch_series(series_id)) for x in met_response
        ]

        return [(x, False) for x in formatted_series_issues_result]

    def fetch_issues_by_series_issue_num_and_year(
        self, series_id_list: list[str], issue_number: str, year: str | int | None
    ) -> list[GenericMetadata]:
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

            for issue in met_response:
                issues_result.append(self._map_comic_issue_to_metadata(issue, self._fetch_series(int(series_id))))

        return issues_result

    def _get_metron_content(
        self, endpoint: str, params: dict[str, Any] | int
    ) -> list[Series] | list[Issue] | Issue | Series | SeriesList | IssuesList:
        """Use the mokkari python library to retrieve data from Metron.cloud"""
        try:
            metron_api = mokkari.api(
                self.username,
                self.user_password,
                cache=mokkari.sqlite_cache.SqliteCache(str(self.cache_folder / "metron_cache.db"), expire=7),
                user_agent="comictagger/" + self.version,
            )
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
            pub = ""
            if getattr(record, "publisher", None):
                pub = record.publisher.name
            # Option to use sort name?
            series_name = ""
            if getattr(record, "name", None):
                series_name = record.name
            else:
                # display_name contains (year) which will mess up fuzzy search results
                series_name = re.split(r"\(\d{4}\)$", record.display_name)[0].strip()

            formatted_results.append(
                ComicSeries(
                    aliases=[],
                    count_of_issues=record.issue_count,
                    count_of_volumes=None,
                    description="",
                    id=str(record.id),
                    image_url="",
                    name=series_name,
                    publisher=pub,
                    start_year=record.year_began,
                    genres=[],
                    format=None,
                )
            )

        return formatted_results

    def _format_series(self, search_result: Series) -> ComicSeries:
        pub_name = search_result.publisher.name

        genres = []
        for genre in search_result.genres:
            genres.append(genre.name)

        # Option to use sort name?
        series_name = search_result.name
        # Put sort name in aliases for now
        aliases = []
        if search_result.sort_name:
            aliases.append(search_result.sort_name)

        desc = search_result.desc

        img = ""
        # To work around API not returning a series image, associated may have image under id -999
        for assoc in search_result.associated:
            if assoc.id == -999:
                img = assoc.name

        formatted_result = ComicSeries(
            aliases=aliases,
            count_of_issues=search_result.issue_count,
            count_of_volumes=None,
            genres=genres,
            description=desc,
            id=str(search_result.id),
            image_url=img,
            name=series_name,
            publisher=pub_name,
            format="",
            start_year=search_result.year_began,
        )

        return formatted_result

    def fetch_series(self, series_id: int) -> ComicSeries:
        return self._format_series(self._fetch_series(series_id))

    def _fetch_series(self, series_id: int) -> Series:
        # mokkari handles cache
        met_response: Series = self._get_metron_content("series", series_id)

        # False by default, causes delay in showing series window due to fetching issue list for series
        if self.find_series_covers:
            series_image = self._fetch_series_cover(series_id, met_response.issue_count)
            # Insert a series image (even if it's empty). Will misuse the associated series field
            met_response.associated.append(AssociatedSeries(id=-999, name=series_image))

        return met_response

    def _fetch_issue_data(self, series_id: int, issue_number: str) -> GenericMetadata:
        # Have to search for an IssueList as Issue is only usable via ID
        met_response: IssuesList = self._get_metron_content(
            "issues_list", {"series_id": series_id, "number": issue_number}
        )
        if len(met_response.issues) > 0:
            # Presume only one result
            return self._fetch_issue_data_by_issue_id(met_response.issues[0].id)

        return GenericMetadata()

    def _fetch_issue_data_by_issue_id(self, issue_id: str) -> GenericMetadata:
        met_response: Issue = self._get_metron_content("issue", int(issue_id))

        # Get full series info
        series_data: Series = self._fetch_series(met_response.series.id)

        # Now, map the GenericMetadata data to generic metadata
        return self._map_comic_issue_to_metadata(met_response, series_data)

    def _fetch_series_cover(self, series_id: int, issue_count: int) -> str:
        # Metron/Mokkari does not return an image for the series therefore fetch the first issue cover

        met_response: IssuesList = self._get_metron_content("issues_list", {"series_id": series_id})

        # Inject a series cover image
        img = ""
        # Take the first record, it should be issue 1 if the series starts at issue 1
        if len(met_response) > 0:
            img = met_response[0].image

        return img

    def _map_comic_issue_to_metadata(self, issue: Issue, series: Series) -> GenericMetadata:
        # Cover both IssueList (with less data) and Issue
        md = GenericMetadata(
            tag_origin=TagOrigin(self.id, self.name),
            issue_id=utils.xlate(issue.id),
            series_id=utils.xlate(series.id),
            title_aliases=[],
            publisher=utils.xlate(series.publisher.name),
            issue=utils.xlate(IssueString(issue.number).as_string()),
            series=utils.xlate(series.name),
        )

        if issue.image is None:
            md.cover_image = ""
        else:
            md.cover_image = issue.image

        # Check if series is ongoing to legitimise issue count OR use option setting
        if hasattr(series, "issue_count"):
            if hasattr(series, "series_type"):
                # 1 = Ongoing, 2 = cancelled, 5 = One-shot, 6 = Annual Series, 8 = Hard Cover, 9 = Graphic Novel
                # 10 = Trade Paperback, 11 = Limited series
                if series.series_type.id != 1:
                    md.issue_count = utils.xlate_int(series.issue_count)
            # This is better than going down an if rabbit hole?
            if self.use_ongoing_issue_count:
                md.issue_count = utils.xlate_int(series.issue_count)

        if hasattr(series, "series_type"):
            # 5 = One-shot, 6 = Annual Series, 8 = Hard Cover, 9 = Graphic Novel, 10 = Trade Paperback,
            # 11 = Limited Series
            if series.series_type.id == 5:
                md.format = series.series_type.name
            if series.series_type.id == 6:
                md.format = series.series_type.name
            if series.series_type.id == 8:
                md.format = series.series_type.name
            if series.series_type.id == 9:
                md.format = series.series_type.name
            if series.series_type.id == 10:
                md.format = "TPB"
            if series.series_type.id == 11:
                md.format = series.series_type.name

        if hasattr(issue, "desc"):
            md.description = issue.desc

        if hasattr(series, "genres"):
            genres = []
            for genre in series.genres:
                genres.append(genre.name)
            md.genres = genres

        #  issue_name is only for IssueList, it's just the series name is issue number
        if hasattr(issue, "issue_name"):
            md.title = utils.xlate(issue.issue_name)

        # If there is a collection_title (for TPB) there should be no story_titles?
        if hasattr(issue, "collection_title"):
            md.title = utils.xlate(issue.collection_title)

        if hasattr(issue, "story_titles"):
            if len(issue.story_titles) > 0:
                md.title = "; ".join(issue.story_titles)

        if hasattr(issue, "rating"):
            md.maturity_rating = issue.rating.name

        if hasattr(issue, "resource_url"):
            md.web_link = issue.resource_url

        md.alternate_images = []
        if hasattr(issue, "variants"):
            for alt in issue.variants:
                md.alternate_images.append(alt.image)

        md.characters = []
        if hasattr(issue, "characters"):
            for character in issue.characters:
                md.characters.append(character.name)

        md.teams = []
        if hasattr(issue, "teams"):
            for team in issue.teams:
                md.teams.append(team.name)

        md.story_arcs = []
        if hasattr(issue, "arcs"):
            for arc in issue.arcs:
                md.story_arcs.append(arc.name)

        if hasattr(issue, "credits"):
            for person in issue.credits:
                md.add_credit(person.creator, person.role[0].name.title().strip(), False)

        md.volume = utils.xlate_int(issue.series.volume)
        if self.use_series_start_as_volume:
            md.volume = series.year_began

        if hasattr(issue, "price"):
            md.price = utils.xlate_float(issue.price)

        if hasattr(issue, "cover_date"):
            if issue.cover_date:
                md.day, md.month, md.year = utils.parse_date_str(issue.cover_date.strftime("%Y-%m-%d"))
            elif series.year_began:
                md.year = utils.xlate_int(series.year_began)

        return md

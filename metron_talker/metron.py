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
import decimal
import json
import logging
import pathlib
import re
from datetime import date, datetime
from enum import Enum
from typing import Any, Callable

import mokkari
import settngs
from comicapi import utils
from comicapi.genericmetadata import ComicSeries, GenericMetadata, TagOrigin
from comicapi.issuestring import IssueString
from comictalker.comiccacher import ComicCacher
from comictalker.comiccacher import Issue as CCIssue
from comictalker.comiccacher import Series as CCSeries
from comictalker.comictalker import ComicTalker, TalkerNetworkError
from mokkari.issue import Issue, IssueSchema, IssuesList
from mokkari.series import AssociatedSeries, Series, SeriesList, SeriesSchema

logger = logging.getLogger(f"comictalker.{__name__}")


class MetronSeriesType(Enum):
    ongoing = 1
    one_shot = 5
    annual_series = 6
    hard_cover = 8
    graphic_novel = 9
    trade_paperback = 10
    limited_series = 11


class MetronEncoder(json.JSONEncoder):
    def default(self, obj: Any) -> Any:
        if isinstance(obj, (datetime, date)):
            return obj.isoformat()
        if isinstance(obj, decimal.Decimal):
            return str(obj.real)

        # There is a disparity between key names from the API and Mokkari, adjust here when saving cache
        if isinstance(obj, Issue):
            new_obj = obj.__dict__
            if hasattr(obj, "publisher"):
                # Can presume full Issue and not IssueList
                new_obj["title"] = obj.collection_title
                new_obj["name"] = obj.story_titles
                new_obj["page"] = obj.page_count
            else:
                new_obj["issue"] = obj.issue_name
                del new_obj["issue_name"]
            return new_obj

        if isinstance(obj, Series):
            new_obj = obj.__dict__
            if hasattr(obj, "display_name"):
                new_obj["series"] = obj.display_name
            return new_obj

        if isinstance(obj, AssociatedSeries):
            return {"id": obj.id, "series": obj.name}

        # Everything else return as dict
        return obj.__dict__


class MetronTalker(ComicTalker):
    name: str = "Metron"
    id: str = "metron"
    comictagger_min_ver: str = "1.6.0a7"
    website: str = "https://metron.cloud"
    logo_url: str = "https://static.metron.cloud/static/site/img/metron.svg"
    attribution: str = f"Metadata provided by <a href='{website}'>{name}</a>"
    about: str = (
        f"<a href='{website}'>{name}</a> is a community-based site whose goal is to build an open database "
        f"with a REST API for comic books. <p>NOTE: An account on <a href='{website}'>{name}</a> is "
        f"required to use its API.</p><p>NOTE: Automatic image comparisons are not available due to the"
        f"extra bandwidth require. Donations will be accepted soon, check the website.</p>"
    )

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

        self.mokkari_api = None

    def register_settings(self, parser: settngs.Manager) -> None:
        parser.add_setting(
            "--met-use-series-start-as-volume",
            default=False,
            action=argparse.BooleanOptionalAction,
            display_name="Use series start as volume",
            help="Use the series start year as the volume number",
        )
        parser.add_setting(
            "--met-series-covers",
            default=False,
            cmdline=False,
            action=argparse.BooleanOptionalAction,
            display_name="Attempt to fetch a cover for each series",
            help="Fetches a cover for each series in the series selection window",
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
        self.find_series_covers = settings["met_series_covers"]
        self.use_ongoing_issue_count = settings["met_use_ongoing"]
        self.username = settings["met_username"]
        self.user_password = settings["metron_key"]

        # If the username and password is invalid, the talker will not initialise
        try:
            self.mokkari_api = mokkari.api(self.username, self.user_password, user_agent="comictagger/" + self.version)
        except Exception:
            pass

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
        logger.info(f"{self.name} searching: {series_name}")

        # Before we search online, look in our cache, since we might have done this same search recently
        # For literal searches always retrieve from online
        cvc = ComicCacher(self.cache_folder, self.version)
        if not refresh_cache and not literal:
            cached_search_results = cvc.get_search_results(self.id, series_name)
            if len(cached_search_results) > 0:
                # The SeriesList is expected to be in "results"
                json_cache = {"results": [json.loads(x[0].data) for x in cached_search_results]}
                return self._format_search_results(SeriesList(json_cache))

        met_response: SeriesList = self._get_metron_content("series_list", {"name": series_name})

        # Cache these search results, even if it's literal we cache the results
        # The most it will cause is extra processing time
        cvc.add_search_results(
            self.id,
            series_name,
            [CCSeries(id=str(x.id), data=json.dumps(x, cls=MetronEncoder)) for x in met_response],
            False,
        )

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
        # before we search online, look in our cache, since we might already have this info
        cvc = ComicCacher(self.cache_folder, self.version)
        cached_series_issues_result = cvc.get_series_issues_info(str(series_id), self.id)

        # Need the issue count to check against the cached issue list
        series_data: Series = self._fetch_series(int(series_id))

        # Check cache length against count of issues in case a new issue
        if len(cached_series_issues_result) == series_data.issue_count:
            # issue number, year, month or cover_image
            # If any of the above are missing it will trigger a call to fetch_comic_data for the full issue info.
            # Because variants are only returned via a full issue call, remove the image URL to trigger this.
            if self.display_variants:
                cache_data: list[tuple[Issue, bool]] = []
                for issue in cached_series_issues_result:
                    cache_data.append((IssueSchema().load(json.loads(issue[0].data)), issue[1]))
                for issue_data in cache_data:
                    # Check to see if it's a full record before emptying image
                    if not issue_data[1]:
                        issue_data[0].image = ""
                return [self._map_comic_issue_to_metadata(x[0], series_data) for x in cache_data]
            return [
                self._map_comic_issue_to_metadata(IssueSchema().load(json.loads(x[0].data)), series_data)
                for x in cached_series_issues_result
            ]

        met_response: IssuesList = self._get_metron_content("issues_list", {"series_id": series_id})

        # Cache these search results, even if it's literal we cache the results
        # The most it will cause is extra processing time
        cvc.add_issues_info(
            self.id,
            [CCIssue(id=str(x.id), series_id=series_id, data=json.dumps(x, cls=MetronEncoder)) for x in met_response],
            False,
        )

        # Same variant covers mechanism as above. This should only affect the GUI
        if self.display_variants:
            for issue in met_response:
                issue.image = ""

        # Format to expected output
        formatted_series_issues_result = [self._map_comic_issue_to_metadata(x, series_data) for x in met_response]

        return formatted_series_issues_result

    def fetch_issues_by_series_issue_num_and_year(
        self, series_id_list: list[str], issue_number: str, year: str | int | None
    ) -> list[GenericMetadata]:
        # At the request of Metron, we will not retrieve the variant covers for matching
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
            series = self._fetch_series(int(series_id))

            for issue in met_response:
                # Remove image URL to comply with Metron's request due to high bandwidth usage
                issue.image = ""
                issues_result.append(self._map_comic_issue_to_metadata(issue, series))

        return issues_result

    def _get_metron_content(
        self, endpoint: str, params: dict[str, Any] | int
    ) -> list[Series] | list[Issue] | Issue | Series | SeriesList | IssuesList:
        """Use the mokkari python library to retrieve data from Metron.cloud"""
        try:
            if self.mokkari_api is None:
                raise TalkerNetworkError(self.name, 2, "Invalid username or password")
            result = getattr(self.mokkari_api, endpoint)(params)
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
            img = ""
            # To work around API not returning a series image, associated may have image under id -999
            if hasattr(search_results, "associated"):
                for assoc in search_results.associated:
                    if assoc.id == -999:
                        img = assoc.name

            pub = ""
            if hasattr(record, "publisher"):
                pub = record.publisher.name

            # Option to use sort name?
            series_name = ""
            if hasattr(record, "name"):
                series_name = record.name
            else:
                # display_name contains (year) which will mess up fuzzy search results
                series_name = re.split(r"\(\d{4}\)$", record.display_name)[0].strip()

            formatted_results.append(
                ComicSeries(
                    aliases=set(),
                    count_of_issues=record.issue_count,
                    count_of_volumes=None,
                    description="",
                    id=str(record.id),
                    image_url=img,
                    name=series_name,
                    publisher=pub,
                    start_year=record.year_began,
                    format=None,
                )
            )

        return formatted_results

    def _format_series(self, search_result: Series) -> ComicSeries:
        # Option to use sort name?
        # Put sort name in aliases for now

        img = ""
        # To work around API not returning a series image, associated may have image under id -999
        for assoc in search_result.associated:
            if assoc.id == -999:
                img = assoc.name

        formatted_result = ComicSeries(
            aliases=set(search_result.sort_name),
            count_of_issues=search_result.issue_count,
            count_of_volumes=None,
            description=search_result.desc,
            id=str(search_result.id),
            image_url=img,
            name=search_result.name,
            publisher=search_result.publisher.name,
            format="",
            start_year=search_result.year_began,
        )

        return formatted_result

    def fetch_series(self, series_id: int) -> ComicSeries:
        return self._format_series(self._fetch_series(series_id))

    def _fetch_series(self, series_id: int) -> Series:
        cvc = ComicCacher(self.cache_folder, self.version)
        cached_series_result = cvc.get_series_info(str(series_id), self.id)

        if cached_series_result is not None and cached_series_result[1]:
            # Check if series cover attempt was made if option is set
            cache_data: Series = SeriesSchema().load(json.loads(cached_series_result[0].data))
            if not self.find_series_covers:
                return cache_data
            if self.find_series_covers:
                for assoc in cache_data.associated:
                    if assoc.id == -999:
                        return cache_data
                        # Did not find a series cover, fetch full record

        met_response: Series = self._get_metron_content("series", series_id)

        # False by default, causes delay in showing series window due to fetching issue list for series
        if self.find_series_covers:
            series_image = self._fetch_series_cover(series_id, met_response.issue_count)
            # Insert a series image (even if it's empty). Will misuse the associated series field
            met_response.associated.append(AssociatedSeries(id=-999, name=series_image))

        if met_response:
            cvc.add_series_info(
                self.id,
                CCSeries(id=str(met_response.id), data=json.dumps(met_response, cls=MetronEncoder).encode("utf-8")),
                True,
            )

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
        cvc = ComicCacher(self.cache_folder, self.version)
        cached_issues_result = cvc.get_issue_info(int(issue_id), self.id)

        if cached_issues_result and cached_issues_result[1]:
            return self._map_comic_issue_to_metadata(
                (IssueSchema().load(json.loads(cached_issues_result[0].data))),
                self._fetch_series(int(cached_issues_result[0].series_id)),
            )

        met_response: Issue = self._get_metron_content("issue", int(issue_id))

        # Get full series info
        series_data: Series = self._fetch_series(met_response.series.id)

        cvc.add_issues_info(
            self.id,
            [
                CCIssue(
                    id=str(met_response.id),
                    series_id=str(series_data.id),
                    data=json.dumps(met_response, cls=MetronEncoder).encode("utf-8"),
                )
            ],
            True,
        )

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
            publisher=utils.xlate(series.publisher.name),
            issue=utils.xlate(IssueString(issue.number).as_string()),
            series=utils.xlate(series.name),
        )

        md._cover_image = issue.image

        series_type = -1
        if hasattr(series, "series_type"):
            series_type = series.series_type.id

        # Check if series is ongoing to legitimise issue count OR use option setting
        if hasattr(series, "issue_count"):
            if series_type != MetronSeriesType.ongoing.value or self.use_ongoing_issue_count:
                md.issue_count = utils.xlate_int(series.issue_count)

        if series_type in (
            MetronSeriesType.annual_series.value,
            MetronSeriesType.graphic_novel.value,
            MetronSeriesType.hard_cover.value,
            MetronSeriesType.limited_series.value,
            MetronSeriesType.one_shot.value,
        ):
            md.format = series.series_type.name
        if series_type == MetronSeriesType.trade_paperback.value:
            md.format = "TPB"

        md.description = getattr(issue, "desc", None)

        if hasattr(series, "genres"):
            genres = []
            for genre in series.genres:
                genres.append(genre.name)
            md.genres = set(genres)

        #  issue_name is only for IssueList, it's just the series name is issue number, we'll ignore it

        # If there is a collection_title (for TPB) there should be no story_titles?
        md.title = utils.xlate(getattr(issue, "collection_title", ""))

        if hasattr(issue, "story_titles"):
            if len(issue.story_titles) > 0:
                md.title = "; ".join(issue.story_titles)

        if hasattr(issue, "rating") and issue.rating.name != "Unknown":
            md.maturity_rating = issue.rating.name

        md.web_link = getattr(issue, "resource_url", None)

        if hasattr(issue, "variants"):
            for alt in issue.variants:
                md._alternate_images.append(alt.image)

        if hasattr(issue, "characters"):
            for character in issue.characters:
                md.characters.add(character.name)

        if hasattr(issue, "teams"):
            for team in issue.teams:
                md.teams.add(team.name)

        if hasattr(issue, "arcs"):
            for arc in issue.arcs:
                md.story_arcs.append(arc.name)

        if hasattr(issue, "credits"):
            for person in issue.credits:
                md.add_credit(person.creator, person.role[0].name.title().strip(), False)

        md.volume = utils.xlate_int(issue.series.volume)
        if self.use_series_start_as_volume:
            md.volume = series.year_began

        md.price = utils.xlate_float(getattr(issue, "price", ""))

        if hasattr(issue, "cover_date"):
            if issue.cover_date:
                md.day, md.month, md.year = utils.parse_date_str(issue.cover_date.strftime("%Y-%m-%d"))
            elif series.year_began:
                md.year = utils.xlate_int(series.year_began)

        return md

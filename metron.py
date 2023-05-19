"""
Metron.cloud information source
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
import json
import logging
import pathlib
import time
from typing import Any, Callable, Generic, TypeVar
from urllib.parse import urljoin

import requests
from requests.auth import HTTPBasicAuth
from pyrate_limiter import Duration, RequestRate, Limiter

#import mokkari

import settngs
from typing_extensions import Required, TypedDict

import comictalker.talker_utils as talker_utils
from comicapi import utils
from comicapi.genericmetadata import GenericMetadata
from comicapi.issuestring import IssueString
from comictalker.comiccacher import ComicCacher
from comictalker.comictalker import ComicTalker, TalkerDataError, TalkerNetworkError
from comictalker.resulttypes import ComicIssue, ComicSeries, Credit

logger = logging.getLogger(__name__)


class MetArc(TypedDict):
    id: int
    name: str
    desc: str
    image: str
    resource_url: str
    modified: str


class MetTeam(TypedDict):
    id: int
    name: str
    desc: str
    image: str
    creators: list[MetCreator]
    resource_url: str
    modified: str


class MetCreator(TypedDict):
    id: int
    name: str
    birth: str
    death: str
    desc: str
    image: str
    alias: list[str]
    resource_url: str
    modified: str


class MetCharacter(TypedDict):
    id: int
    name: str
    alias: list[str]
    desc: str
    image: str
    creators: list[MetCreator]
    teams: list[MetTeam]
    resource_url: str
    modified: str


class MetPublisher(TypedDict):
    id: int
    name: str
    founded: int
    desc: str
    image: str
    resource_url: str
    modified: str


class MetGenre(TypedDict):
    id: int
    name: str


class MetRating(TypedDict):
    id: int
    name: str


class MetRole(TypedDict):
    id: int
    name: str


class MetCredit(TypedDict):
    id: int
    creator: str
    role: list[MetRole]


class MetReprint(TypedDict):
    id: int
    name: str


class MetVariant(TypedDict):
    name: str
    sku: str
    upc: str
    image: str


class MetSeriesType(TypedDict):
    id: int
    name: str


class MetAssociated(TypedDict):
    id: int
    series: str


class MetIssue(TypedDict, total=False):
    id: int
    publisher: MetPublisher
    series: MetSeries
    genres: list[MetGenre]
    number: str
    title: str
    name: list[str]
    cover_date: str
    store_date: str
    price: str
    rating: MetRating
    sku: str
    isdn: str
    ups: str
    page: int
    desc: str
    image: str
    arcs: list[MetArc]
    credits: list[MetCredit]
    characters: list[MetCharacter]
    teams: list[MetTeam]
    reprints: list[MetReprint]
    variants: list[MetVariant]
    resource_url: str
    modified: str


class MetSeries(TypedDict, total=False):
    id: int
    name: str
    sort_name: str
    volume: int
    series_type: MetSeriesType
    publisher: MetPublisher
    year_began: int
    year_end: int
    desc: str
    issue_count: int
    genres: list[MetGenre]
    associated: list[MetAssociated]
    resource_url: str
    modified: str


class MetSeriesList(TypedDict):
    id: int
    series: str
    year_began: int
    number: int
    issue_count: int
    modified: str


class MetIssueListSeries(TypedDict):
    name: str
    volume: int
    year_began: int


class MetIssueList(TypedDict):
    id: int
    series: MetIssueListSeries
    number: int
    issue: str
    cover_date: str
    image: str
    modified: str


class MetResult(TypedDict):
    count: int
    next: str
    previous: str
    results: list[MetSeriesList] | list[MetIssueList]


# Metron has a limit of 30 calls per minute
limiter = Limiter(RequestRate(30, Duration.MINUTE))


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
        self.remove_html_tables: bool = False
        self.use_series_start_as_volume: bool = False

    def register_settings(self, parser: settngs.Manager) -> None:
        parser.add_setting(
            "--met-use-series-start-as-volume",
            default=False,
            action=argparse.BooleanOptionalAction,
            display_name="Use series start as volume",
            help="Use the series start year as the volume number",
        )
        parser.add_setting(
            "--met-remove-html-tables",
            default=False,
            action=argparse.BooleanOptionalAction,
            display_name="Remove HTML tables",
            help="Removes html tables instead of converting them to text",
        )
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
        parser.add_setting(
            f"--{self.id}-url",
            default="",
            display_name="API URL",
            help=f"Use the given Metron URL. (default: {self.default_api_url})",
        )

    def parse_settings(self, settings: dict[str, Any]) -> dict[str, Any]:
        settings = super().parse_settings(settings)

        self.username = settings["met_username"]
        # self.user_password = settings["met_user_password"]
        return settings

    def check_api_key(self, url: str, key: str) -> tuple[str, bool]:
        url = talker_utils.fix_url(url)
        if not url:
            url = self.default_api_url
        try:
            test_url = urljoin(url, "series/1")

            met_response: MetSeries = requests.get(
                test_url,
                headers={"user-agent": "comictagger/" + self.version},
                auth=HTTPBasicAuth(self.username, key),
            ).json()

            if met_response.get("detail"):
                return met_response["detail"], False

            return "The API access test was successful", True

        except Exception:
            return "Failed to connect to the API! Incorrect URL?", False

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

        params = {
            "name": search_series_name,
            "page": 1,
        }

        met_response: MetResult[list[MetSeriesList]] = self._get_cv_content(urljoin(self.api_url, "series"), params)

        search_results: list[MetSeriesList] = []

        current_result_count = len(met_response["results"])
        total_result_count = met_response["count"]

        # 1. Don't fetch more than some sane amount of pages.
        # 2. Halt when any result on the current page is less than or equal to a set ratio using thefuzz
        max_results = 500  # 5 pages

        total_result_count = min(total_result_count, max_results)

        if callback is None:
            logger.debug(
                f"Found {current_result_count} of {total_result_count} results"
            )
        search_results.extend(met_response["results"])
        page = 1

        if callback is not None:
            callback(len(met_response["results"]), total_result_count)

        # see if we need to keep asking for more pages...
        while current_result_count < met_response['count']:
            if not literal:
                # Stop searching once any entry falls below the threshold
                stop_searching = any(
                    not utils.titles_match(search_series_name, series["series"], series_match_thresh)
                    for series in met_response["results"]
                )

                if stop_searching:
                    break

            if callback is None:
                logger.debug(f"getting another page of results {page * 100} of {total_result_count}...")
            page += 1

            params["page"] = page
            met_response = self._get_cv_content(urljoin(self.api_url, "series/"), params)

            search_results.extend(met_response["results"])
            current_result_count += len(met_response["results"])

            if callback is not None:
                callback(current_result_count, total_result_count)

        # Format result to ComicIssue
        formatted_search_results = self._format_search_results(search_results)

        # Cache these search results, even if it's literal we cache the results
        # The most it will cause is extra processing time
        cvc.add_search_results(self.id, series_name, formatted_search_results)

        return formatted_search_results

    def fetch_comic_data(
            self, issue_id: str | None = None, series_id: str | None = None, issue_number: str = ""
    ) -> GenericMetadata:
        comic_data = GenericMetadata()
        if issue_id:
            comic_data = self._fetch_issue_data_by_issue_id(issue_id)
        elif issue_number and series_id:
            comic_data = self._fetch_issue_data(int(series_id), issue_number)

        return comic_data

    def fetch_issues_by_series(self, series_id: str) -> list[ComicIssue]:
        # before we search online, look in our cache, since we might already have this info
        cvc = ComicCacher(self.cache_folder, self.version)
        cached_series_issues_result = cvc.get_series_issues_info(series_id, self.id)

        series_data = self._fetch_series_data(int(series_id))

        if len(cached_series_issues_result) == series_data.count_of_issues:
            return cached_series_issues_result

        # https://metron.cloud/api/issue/?series_id={id}

        params = {
            "series_id": series_id,
            "page": 1,
        }
        met_response: MetResult[list[MetIssue]] = self._get_cv_content(urljoin(self.api_url, "issue/"), params)

        current_result_count = len(met_response["results"])
        total_result_count = met_response["count"]

        series_issues_result = met_response["results"]
        page = 1

        # see if we need to keep asking for more pages...
        while current_result_count < total_result_count:
            page += 1
            params["page"] = page
            met_response = self._get_cv_content(urljoin(self.api_url, "issue/"), params)

            series_issues_result.extend(met_response["results"])
            current_result_count += len(met_response["results"])

        # Format to expected output
        # Pass along series id as issue list won't contain it
        formatted_series_issues_result = self._format_issue_results(series_issues_result, False, int(series_id))

        cvc.add_series_issues_info(self.id, formatted_series_issues_result)

        return formatted_series_issues_result

    def fetch_issues_by_series_issue_num_and_year(
            self, series_id_list: list[str], issue_number: str, year: str | int | None
    ) -> list[ComicIssue]:

        int_year = utils.xlate_int(year)
        if int_year is not None:
            year = str(int_year)

        for series_id in series_id_list:
            params = {
                "series_id": series_id,
                "number": issue_number,
                "page": 1,
            }

            if int_year:
                params["cover_year"]: year

            met_response: MetResult[list[MetIssue]] = self._get_cv_content(urljoin(self.api_url, "issue/"), params)

            current_result_count = len(met_response["results"])
            total_result_count = met_response["count"]

            filtered_issues_result = met_response["results"]
            page = 1

            # see if we need to keep asking for more pages...
            while current_result_count < total_result_count:
                page += 1

                params["page"] = page
                met_response = self._get_cv_content(urljoin(self.api_url, "issue/"), params)

                filtered_issues_result.extend(met_response["results"])
                current_result_count += len(met_response["results"])

        # Pass along series id as issue list does not contain it
        formatted_filtered_issues_result = self._format_issue_results(filtered_issues_result, False, int(series_id))

        return formatted_filtered_issues_result

    def _get_cv_content(self, url: str, params: dict[str, Any]) -> MetResult:
        while True:
            met_response: MetResult = self._get_url_content(url, params)
            if met_response.get("detail"):
                logger.debug(
                    f"{self.name} query failed with error: {met_response['detail']}"
                )
                raise TalkerNetworkError(self.name, 0, f"{met_response['detail']}")

            # it's all good
            break
        return met_response

    @limiter.ratelimit("metron", delay=True)
    def _get_url_content(self, url: str, params: dict[str, Any]) -> Any:
        #metron_api = mokkari.api(self.username, self.user_password)
        #test = metron_api.series_list(params)
        # connect to server:
        # if there is a 500 error, try a few more times before giving up
        # any other error, just bail
        for tries in range(3):
            try:
                resp = requests.get(url, params=params, headers={"user-agent": "comictagger/" + self.version}, auth=HTTPBasicAuth(self.username, self.api_key))
                if resp.status_code == 200:
                    if resp.headers["Content-Type"].split(";")[0] == "text/html":
                        logger.debug("Request exception: Returned text/html. Most likely a 404 error page.")
                        raise TalkerNetworkError(self.name, 0, "Request exception: Returned text/html. Most likely a 404 error page.")
                    return resp.json()
                if resp.status_code == 500:
                    logger.debug(f"Try #{tries + 1}: ")
                    time.sleep(1)
                    logger.debug(str(resp.status_code))
                if resp.status_code == 403:
                    logger.debug("Access denied. Wrong username or password?")
                    raise TalkerNetworkError(self.name, 1, "Access denied. Wrong username or password?")
                if resp.status_code == 401:
                    logger.debug("Access denied. Invalid username or password.")
                    raise TalkerNetworkError(self.name, 1, "Access denied. Invalid username or password.")
                else:
                    break

            except requests.exceptions.Timeout:
                logger.debug(f"Connection to {self.name} timed out.")
                raise TalkerNetworkError(self.name, 4)
            except requests.exceptions.RequestException as e:
                logger.debug(f"Request exception: {e}")
                raise TalkerNetworkError(self.name, 0, str(e)) from e
            except json.JSONDecodeError as e:
                logger.debug(f"JSON decode error: {e}")
                raise TalkerDataError(self.name, 2, "ComicVine did not provide json")

        raise TalkerNetworkError(self.name, 5)

    # Search results and full series data
    def _format_search_results(self, search_results: list[MetSeries] | list[MetSeriesList]) -> list[ComicSeries]:
        formatted_results = []
        for record in search_results:
            # Flatten publisher to name only
            if record.get("publisher") is None:
                pub_name = ""
            else:
                pub_name = record["publisher"].get("name", "")

            start_year = utils.xlate_int(record.get("year_began", ""))

            # TODO Add genres and volume when fields have been added to ComicSeries

            # TODO Figure out number of volumes/issues? Use cancelled/ended?

            # Option to use sort name?
            if record.get("series"):
                series_name = record["series"]
            else:
                series_name = record["name"]

            formatted_results.append(
                ComicSeries(
                    aliases=[],
                    count_of_issues=record.get("issue_count", 0),
                    description=record.get("desc", ""),
                    id=str(record["id"]),
                    image_url=record.get("image", ""),
                    name=series_name,
                    publisher=pub_name,
                    start_year=start_year,
                )
            )

        return formatted_results

    def _format_issue_results(self, issue_results: list[MetIssue] | list[MetIssueList], complete: bool = False, series_id: int = 0) -> list[ComicIssue]:
        formatted_results = []
        for record in issue_results:
            # Extract image super and thumb to name only
            if record.get("image") is None:
                image_url = ""
            else:
                image_url = record["image"]

            alt_images_list = []

            # TODO Add genres when fields have been added to ComicIssue

            # TODO Figure out number of issues? Use cancelled/ended?

            character_list = []
            if record.get("characters"):
                for char in record["characters"]:
                    character_list.append(char["name"])

            location_list = []

            teams_list = []
            if record.get("teams"):
                for loc in record["teams"]:
                    teams_list.append(loc["name"])

            story_list = []
            if record.get("arcs"):
                for loc in record["arcs"]:
                    story_list.append(loc["name"])

            persons_list = []
            if record.get("credits"):
                for person in record["credits"]:
                    for role in person["role"]:
                        persons_list.append(Credit(name=person["creator"], role=role["name"]))

            if series_id:
                series = self._fetch_series_data(series_id)
            else:
                series = self._fetch_series_data(record["series"]["id"])

            name = ""
            if record.get("name"):
                name = record["name"][0]

            if record.get("issue"):
                name = record["issue"]

            formatted_results.append(
                ComicIssue(
                    aliases=[],
                    cover_date=record.get("cover_date", ""),
                    description=record.get("desc", ""),
                    id=str(record["id"]),
                    image_url=image_url,
                    issue_number=record["number"],
                    name=name,
                    site_detail_url=record.get("resource_url", ""),
                    series=series,
                    alt_image_urls=alt_images_list,
                    characters=character_list,
                    locations=location_list,
                    teams=teams_list,
                    story_arcs=story_list,
                    credits=persons_list,
                    complete=complete,
                )
            )

        return formatted_results

    def _fetch_series_data(self, series_id: int) -> ComicSeries:
        # before we search online, look in our cache, since we might already have this info
        cvc = ComicCacher(self.cache_folder, self.version)
        cached_series_result = cvc.get_series_info(str(series_id), self.id)

        # Because of the limited amount of data from API, check count_of_issues. A 0 means no full data.
        if cached_series_result is not None and cached_series_result.count_of_issues != 0:
            return cached_series_result

        series_url = urljoin(self.api_url, f"series/{series_id}")

        met_response: MetResult[MetSeries] = self._get_cv_content(series_url, {})

        formatted_series_results = self._format_search_results([met_response])

        if met_response:
            cvc.add_series_info(self.id, formatted_series_results[0])

        return formatted_series_results[0]

    def _fetch_issue_data(self, series_id: int, issue_number: str) -> GenericMetadata:
        issues_list_results = self.fetch_issues_by_series(str(series_id))

        # Loop through issue list to find the required issue info
        f_record = None
        for record in issues_list_results:
            if not IssueString(issue_number).as_string():
                issue_number = "1"
            if (
                    IssueString(record.issue_number).as_string().casefold()
                    == IssueString(issue_number).as_string().casefold()
            ):
                f_record = record
                break

        if f_record and f_record.complete:
            # Cache had full record
            return talker_utils.map_comic_issue_to_metadata(
                f_record, self.name, self.remove_html_tables, self.use_series_start_as_volume
            )

        if f_record is not None:
            return self._fetch_issue_data_by_issue_id(f_record.id)
        return GenericMetadata()

    def _fetch_issue_data_by_issue_id(self, issue_id: str) -> GenericMetadata:
        # before we search online, look in our cache, since we might already have this info
        cvc = ComicCacher(self.cache_folder, self.version)
        cached_issues_result = cvc.get_issue_info(int(issue_id), self.id)

        if cached_issues_result and cached_issues_result.complete:
            return talker_utils.map_comic_issue_to_metadata(
                cached_issues_result,
                self.name,
                self.remove_html_tables,
                self.use_series_start_as_volume,
            )

        issue_url = urljoin(self.api_url, f"issue/{issue_id}")
        met_response: MetResult[MetIssue] = self._get_cv_content(issue_url, {})

        issue_result = met_response

        # Format to expected output
        met_issue = self._format_issue_results([issue_result], True, 0)

        # Copy publisher from issue to series.
        # met_issue[0].series.publisher = met_response["publisher"]["name"]

        cvc.add_series_issues_info(self.id, met_issue)

        # Now, map the ComicIssue data to generic metadata
        return talker_utils.map_comic_issue_to_metadata(
            met_issue[0],
            self.name,
            self.remove_html_tables,
            self.use_series_start_as_volume,
        )

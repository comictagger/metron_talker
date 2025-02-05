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
import json
import logging
import pathlib
import re
import time
import unicodedata
from enum import Enum
from json import JSONDecodeError
from typing import Any, Callable, Generic, TypeVar, cast
from urllib.parse import urljoin

import settngs
from comicapi import utils
from comicapi.genericmetadata import ComicSeries, GenericMetadata, MetadataOrigin
from comicapi.issuestring import IssueString
from comicapi.utils import LocationParseError, parse_url
from comictalker import talker_utils
from comictalker.comiccacher import ComicCacher
from comictalker.comiccacher import Issue as CCIssue
from comictalker.comiccacher import Series as CCSeries
from comictalker.comictalker import ComicTalker, TalkerDataError, TalkerNetworkError
from pyrate_limiter import Duration, Limiter, RequestRate
from typing_extensions import TypedDict  # Workaround bug in 3.9 and 3.10

try:
    import niquests as requests
except ImportError:
    import requests

from requests.auth import HTTPBasicAuth

logger = logging.getLogger(f"comictalker.{__name__}")


class MetronSeriesType(Enum):
    ongoing = 1
    one_shot = 5
    annual_series = 6
    hard_cover = 8
    graphic_novel = 9
    trade_paperback = 10
    limited_series = 11


class MetArc(TypedDict):
    id: int
    name: str
    desc: str
    image: str
    cv_id: int
    gcd_id: int
    resource_url: str
    modified: str


class MetArcList(TypedDict):
    id: int
    name: str
    modified: str


class MetAssociated(TypedDict):
    id: int
    series: str


class MetCharacterList(TypedDict):
    id: int
    name: str
    modified: str


class MetCharacter(TypedDict):
    id: int
    name: str
    alias: list[str]
    desc: str
    image: str
    creators: list[MetCreator]
    teams: list[MetTeam]
    universes: list[MetUniverse]
    cv_id: int
    gcd_id: int
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
    cv_id: int
    gcd_id: int
    resource_url: str
    modified: str


class MetCreatorList(TypedDict):
    id: int
    name: str
    modified: str


class MetCredit(TypedDict):
    id: int
    creator: str
    role: list[MetRole]


class MetGenre(TypedDict):
    id: int
    name: str


class MetImprint(TypedDict):
    id: int
    name: str
    founded: int
    desc: str
    image: str
    cv_id: int
    gcd_id: int
    publisher: MetShortPub
    resource_url: str
    modified: str


class MetShortImp(TypedDict):
    id: int
    name: str


class MetIssueList(TypedDict):
    id: int
    series: MetIssueListSeries
    number: int
    issue: str
    cover_date: str
    store_date: str
    image: str
    cover_hash: str
    modified: str


class MetIssueListSeries(TypedDict):
    name: str
    volume: int
    year_began: int


class MetIssue(TypedDict, total=False):
    id: int
    publisher: MetShortPub
    imprint: MetShortImp
    series: MetSeries
    number: str
    title: str
    issue: str  # IssueList only
    name: list[str]
    cover_date: str
    store_date: str
    price: str
    rating: MetRating
    sku: str
    isdn: str
    upc: str
    page: int
    desc: str
    image: str
    cover_hash: str
    arcs: list[MetArc]
    credits: list[MetCredit]
    characters: list[MetCharacter]
    teams: list[MetTeam]
    universes: list[MetUniverse]
    reprints: list[MetReprint]
    variants: list[MetVariant]
    cv_id: int
    gcd_id: int
    resource_url: str
    modified: str


class MetIssueSeries(TypedDict):
    id: int
    name: int
    sort_name: str
    volume: int
    year_began: str
    series_type: MetSeriesType
    genres: list[MetGenre]


class MetPublisher(TypedDict):
    id: int
    name: str
    founded: int
    desc: str
    image: str
    cv_id: int
    gcd_id: int
    resource_url: str
    modified: str


class MetShortPub(TypedDict):
    id: int
    name: str


class MetRating(TypedDict):
    id: int
    name: str


class MetReprint(TypedDict):
    id: int
    issue: str


class MetRole(TypedDict):
    id: int
    name: str


class MetSeriesList(TypedDict, total=False):
    associated: list[MetAssociated]  # Can be used to store a series cover image
    id: int
    series: str
    year_began: int
    volume: int
    issue_count: int
    modified: str


class MetSeries(TypedDict, total=False):
    id: int
    name: str
    sort_name: str
    series: str  # SeriesList
    volume: int
    series_type: MetSeriesType
    status: str
    publisher: MetPublisher
    imprint: MetImprint
    year_began: int
    year_end: int
    desc: str
    issue_count: int
    genres: list[MetGenre]
    associated: list[MetAssociated]  # Can be used to store a series cover image
    cv_id: int
    gcd_id: int
    resource_url: str
    modified: str


class MetSeriesType(TypedDict):
    id: int
    name: str


class MetTeamList(TypedDict):
    id: int
    name: str
    modified: str


class MetTeam(TypedDict):
    id: int
    name: str
    desc: str
    image: str
    creators: list[MetCreator]
    universes: list[MetUniverse]
    cv_id: int
    gcd_id: int
    resource_url: str
    modified: str


class MetUniverseList(TypedDict):
    id: int
    name: str
    modified: str


class MetUniverse(TypedDict):
    id: int
    publisher: MetPublisher
    name: str
    designation: str
    desc: str
    gcd_id: int
    image: str
    resource_url: str
    modified: str


class MetVariant(TypedDict):
    name: str
    sku: str
    upc: str
    image: str


class MetError(TypedDict):
    detail: str


T = TypeVar("T", list[MetSeries], list[MetIssue])


class MetResult(TypedDict, Generic[T]):
    count: int
    next: str
    previous: str
    results: T


# Metron has a limit of 30 calls per minute
limiter = Limiter(RequestRate(30, Duration.MINUTE))


class MetronTalker(ComicTalker):
    name: str = "Metron"
    id: str = "metron"
    comictagger_min_ver: str = "1.6.0a13"
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
            display_name="API URL",
            help="Use the given Metron URL",
        )

    def parse_settings(self, settings: dict[str, Any]) -> dict[str, Any]:
        settings = super().parse_settings(settings)

        self.use_series_start_as_volume = settings["met_use_series_start_as_volume"]
        self.find_series_covers = settings["met_series_covers"]
        self.use_ongoing_issue_count = settings["met_use_ongoing"]
        self.username = settings["met_username"]
        self.user_password = settings["metron_key"]

        return settings

    def check_status(self, settings: dict[str, Any]) -> tuple[str, bool]:
        url = talker_utils.fix_url(settings["metron_url"])
        if not url:
            url = self.default_api_url
        try:
            test_url = urljoin(url, "series/1/")

            met_response = requests.get(
                test_url,
                headers={"user-agent": "comictagger/" + self.version},
                auth=HTTPBasicAuth(settings["met_username"], settings["metron_key"]),
            )

            if met_response.status_code == 401:
                return "Access denied. Invalid username or password.", False
            if met_response.status_code == 404:
                return f"Possible website error or incorrect URL: {test_url}", False
            if met_response.status_code == 200:
                met_response = met_response.json()
                if met_response.get("detail"):
                    return met_response["detail"], False

            return "The API access test was successful", True

        except JSONDecodeError:
            return f"Failed to decode JSON. Possible website error or incorrect URL: {test_url}", False
        except Exception as e:
            return f"Failed to connect to the API! {e}", False

    def search_for_series(
        self,
        series_name: str,
        callback: Callable[[int, int], None] | None = None,
        refresh_cache: bool = False,
        literal: bool = False,
        series_match_thresh: int = 90,
    ) -> list[ComicSeries]:
        # Sanitize the series name specially for Metron
        search_series_name = self._sanitize_title(series_name, literal)

        # A literal search was asked for, do not sanitize
        if literal:
            search_series_name = series_name

        logger.info(f"{self.name} searching: {search_series_name}")

        # Before we search online, look in our cache, since we might have done this same search recently
        # For literal searches always retrieve from online
        cvc = ComicCacher(self.cache_folder, self.version)
        if not refresh_cache and not literal:
            cached_search_results = cvc.get_search_results(self.id, search_series_name)
            if len(cached_search_results) > 0:
                # The SeriesList is expected to be in "results"
                json_cache = [json.loads(x[0].data) for x in cached_search_results]
                return self._format_search_results(json_cache)
        logger.debug("Search for %s cached: False", repr(series_name))

        params = {
            "name": search_series_name,
            "page": 1,
        }

        met_response: MetResult[list[MetSeries]] = cast(
            MetResult[list[MetSeries]],
            self._get_metron_content(urljoin(self.api_url, "series/"), params),
        )

        search_results: list[MetSeries] = []

        current_result_count = len(met_response["results"])
        total_result_count = met_response["count"]

        # 1. Don't fetch more than some sane amount of pages.
        # 2. Halt when any result on the current page is less than or equal to a set ratio using thefuzz
        max_results = 500  # 5 pages

        total_result_count = min(total_result_count, max_results)

        if callback is None:
            logger.debug(f"Found {current_result_count} of {total_result_count} results")
        search_results.extend(met_response["results"])
        page = 1

        if callback is not None:
            callback(len(met_response["results"]), total_result_count)

        # see if we need to keep asking for more pages...
        while current_result_count < met_response["count"]:
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
            met_response = cast(
                MetResult[list[MetSeries]],
                self._get_metron_content(urljoin(self.api_url, "series/"), params),
            )

            search_results.extend(met_response["results"])
            current_result_count += len(met_response["results"])

            if callback is not None:
                callback(current_result_count, total_result_count)

        # Format result to ComicIssue
        formatted_search_results = self._format_search_results(search_results)

        # Cache these search results, even if it's literal we cache the results
        # The most it will cause is extra processing time
        cvc.add_search_results(
            self.id,
            search_series_name,
            [CCSeries(id=str(x["id"]), data=json.dumps(x).encode("utf-8")) for x in search_results],
            False,
        )

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
            # Should never hit this but just in case
            comic_data = self._fetch_issue_data(int(series_id), issue_number)

        return comic_data

    def fetch_issues_in_series(self, series_id: str) -> list[GenericMetadata]:
        cache_series_data: MetSeries | None = None
        logger.debug("Fetching all issues in series: %s", series_id)
        # before we search online, look in our cache, since we might already have this info
        cvc = ComicCacher(self.cache_folder, self.version)
        cached_series_issues_result = cvc.get_series_issues_info(str(series_id), self.id)
        cached_series_result = cvc.get_series_info(str(series_id), self.id)

        if cached_series_result is not None:
            cache_series_data = json.loads(cached_series_result[0].data)

        logger.debug(
            "Found %d issues cached",
            len(cached_series_issues_result),
        )
        if cache_series_data is not None:
            logger.debug(" need %d issues", cache_series_data["issue_count"] - len(cached_series_issues_result))

        # Check for stale cache number of issues
        if cache_series_data is not None and (
            len(cached_series_issues_result) == cache_series_data.get("issue_count", -1)
        ):
            # issue number, year, month or cover_image
            # If any of the above are missing it will trigger a call to fetch_comic_data for the full issue info.
            # Because variants are only returned via a full issue call, remove the image URL to trigger this.
            if self.display_variants:
                cache_data: list[tuple[MetIssue, bool]] = []
                for issue in cached_series_issues_result:
                    cache_data.append((json.loads(issue[0].data), issue[1]))
                for issue_data in cache_data:
                    # Check to see if it's a full record before emptying image
                    if not issue_data[1]:
                        issue_data[0]["image"] = ""
                return [self._map_comic_issue_to_metadata(x[0]) for x in cache_data]
            return [self._map_comic_issue_to_metadata(json.loads(x[0].data)) for x in cached_series_issues_result]

        params = {
            "series_id": series_id,
            "page": 1,
        }
        met_response: MetResult[list[MetIssue]] = cast(
            MetResult[list[MetIssue]],
            self._get_metron_content(urljoin(self.api_url, "issue/"), params),
        )

        current_result_count = len(met_response["results"])
        total_result_count = met_response["count"]

        series_issues_result = met_response["results"]
        page = 1

        # see if we need to keep asking for more pages...
        while current_result_count < total_result_count:
            page += 1
            params["page"] = page
            met_response = cast(
                MetResult[list[MetIssue]],
                self._get_metron_content(urljoin(self.api_url, "issue/"), params),
            )

            series_issues_result.extend(met_response["results"])
            current_result_count += len(met_response["results"])

        # Cache these search results, even if it's literal we cache the results
        # The most it will cause is extra processing time
        cvc.add_issues_info(
            self.id,
            [
                CCIssue(id=str(x["id"]), series_id=series_id, data=json.dumps(x).encode("utf-8"))
                for x in series_issues_result
            ],
            False,
        )

        # Same variant covers mechanism as above. This should only affect the GUI
        if self.display_variants:
            for issue in series_issues_result:
                issue.image = ""

        # Format to expected output
        formatted_series_issues_result = [self._map_comic_issue_to_metadata(x) for x in series_issues_result]

        return formatted_series_issues_result

    def fetch_issues_by_series_issue_num_and_year(
        self,
        series_id_list: list[str],
        issue_number: str,
        year: str | int | None,
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

            cvc = ComicCacher(self.cache_folder, self.version)

            cached_series_issues = cvc.get_series_issues_info(str(series_id), self.id)
            issue_found = False
            if len(cached_series_issues) > 0:
                for issue, _ in cached_series_issues:
                    issue_data = cast(MetIssue, json.loads(issue.data))
                    # Inject series_id
                    issue_data["series"]["id"] = int(series_id)
                    if issue_data.get("number") == issue_number:
                        # Remove image URL to comply with Metron's request due to high bandwidth usage
                        issue_data["image"] = ""
                        issues_result.append(
                            self._map_comic_issue_to_metadata(
                                issue_data,
                            ),
                        )
                        issue_found = True
                        break

            if not issue_found:
                # Should only ever be one result
                met_response: MetResult[list[MetIssue]] = cast(
                    MetResult[list[MetIssue]],
                    self._get_metron_content(urljoin(self.api_url, "issue/"), params),
                )

                if met_response["count"] > 0:
                    # Inject series_id
                    met_response["results"][0]["series"]["id"] = int(series_id)
                    # Cache result
                    cvc.add_issues_info(
                        self.id,
                        [
                            CCIssue(
                                str(met_response["results"][0]["id"]),
                                str(series_id),
                                json.dumps(met_response["results"][0]).encode("utf-8"),
                            )
                        ],
                        False,
                    )

                    # Remove image URL to comply with Metron's request due to high bandwidth usage
                    met_response["results"][0]["image"] = ""
                    issues_result.append(
                        self._map_comic_issue_to_metadata(
                            met_response["results"][0],
                        )
                    )

        return issues_result

    def _get_metron_content(self, url: str, params: dict[str, Any]) -> MetResult[T] | MetSeries | MetIssue | MetError:
        with limiter.ratelimit(
            "metron",
            delay=True,
        ):
            met_response: MetResult[T] | MetSeries | MetIssue | MetError = self._get_url_content(url, params)
            if met_response.get("detail"):
                met_response = cast(MetError, met_response)
                logger.debug(f"{self.name} query failed with error: {met_response['detail']}")
                raise TalkerNetworkError(self.name, 0, f"{met_response['detail']}")

            return met_response

    def _get_url_content(self, url: str, params: dict[str, Any]) -> Any:
        for tries in range(3):
            try:
                resp = requests.get(
                    url,
                    params=params,
                    headers={"user-agent": "comictagger/" + self.version},
                    auth=HTTPBasicAuth(self.username, self.api_key),
                )
                if resp.status_code == 200:
                    if resp.headers["Content-Type"].split(";")[0] == "text/html":
                        logger.debug("Request exception: Returned text/html. Most likely a 404 error page.")
                        raise TalkerNetworkError(
                            self.name, 0, "Request exception: Returned text/html. Most likely a 404 error page."
                        )
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
                raise TalkerDataError(self.name, 2, "Metron did not provide json")

        raise TalkerNetworkError(self.name, 5)

    def _sanitize_title(self, text: str, basic: bool = False) -> str:
        # normalize unicode and convert to ascii. Does not work for everything eg ½ to 1⁄2 not 1/2
        text = unicodedata.normalize("NFKD", text).casefold()
        # No apostophy removal here as "world's" will not be found as "worlds" but "world s" will
        text = text.replace('"', "")
        if not basic:
            # remove all characters that are not a letter, separator (space) or number
            # replace any "dash punctuation" with a space
            # makes sure that batman-superman and self-proclaimed stay separate words
            text = "".join(
                c if unicodedata.category(c)[0] not in "P" else " "
                for c in text
                if unicodedata.category(c)[0] in "LZNP"
            )
            # remove extra space and articles and all lower case
            text = utils.remove_articles(text).strip()

        return text

    def _format_search_results(self, search_results: list[MetSeries]) -> list[ComicSeries]:
        formatted_results = []
        for record in search_results:
            pub: str | None = None

            # Option to use sort name?
            series_name = ""
            if record.get("name"):
                # "name" indicates full series info
                series_name = record["name"]
                if record.get("publisher"):
                    pub = record["publisher"].get("name")
            else:
                # "series" from SeriesList contains (year) which will mess up fuzzy search results
                series_name = re.split(r"\(\d{4}\)$", record["series"])[0].strip()

            img = ""
            # To work around API not returning a series image, associated may have image under id -999
            if record.get("associated"):
                for assoc in record["associated"]:
                    if assoc["id"] == -999:
                        img = assoc["series"]

            formatted_results.append(
                ComicSeries(
                    aliases=set(),
                    count_of_issues=record["issue_count"],
                    count_of_volumes=None,
                    description="",
                    id=str(record["id"]),
                    image_url=img,
                    name=series_name,
                    publisher=pub,
                    start_year=record.get("year_began"),
                    format=None,
                )
            )

        return formatted_results

    def _format_series(self, search_result: MetSeries) -> ComicSeries:
        # Option to use sort name?
        # Put sort name in aliases for now

        img = ""
        # To work around API not returning a series image, associated may have image under id -999
        for assoc in search_result["associated"]:
            if assoc["id"] == -999:
                img = assoc["series"]

        alias = set()
        alias.add(search_result["sort_name"])
        formatted_result = ComicSeries(
            aliases=alias,
            count_of_issues=search_result["issue_count"],
            count_of_volumes=None,
            description=search_result["desc"],
            id=str(search_result["id"]),
            image_url=img,
            name=search_result["name"],
            publisher=search_result["publisher"]["name"],
            format="",
            start_year=search_result["year_began"],
        )

        return formatted_result

    def fetch_series(
        self,
        series_id: int,
    ) -> ComicSeries:
        return self._format_series(self._fetch_series(series_id))

    def _fetch_series(
        self,
        series_id: int,
    ) -> MetSeries:
        logger.debug("Fetching series info: %s", series_id)

        cvc = ComicCacher(self.cache_folder, self.version)
        cached_series_result = cvc.get_series_info(str(series_id), self.id)

        logger.debug("Series cached: %s", bool(cached_series_result))

        if cached_series_result is not None and cached_series_result[1]:
            # Check if series cover attempt was made if option is set
            cache_data: MetSeries = json.loads(cached_series_result[0].data)
            if not self.find_series_covers:
                return cache_data
            if self.find_series_covers:
                for assoc in cache_data["associated"]:
                    if assoc["id"] == -999:
                        return cache_data
                        # Did not find a series cover, fetch full record

        series_url = urljoin(self.api_url, f"series/{series_id}/")

        met_response: MetSeries = cast(MetSeries, self._get_metron_content(series_url, {}))

        # False by default, causes delay in showing series window due to fetching issue list for series
        if self.find_series_covers:
            series_image = self._fetch_series_cover(series_id, met_response["issue_count"])
            # Insert a series image (even if it's empty). Will misuse the associated series field
            met_response["associated"].append({"id": -999, "series": series_image})

        if met_response:
            cvc.add_series_info(
                self.id,
                CCSeries(id=str(met_response["id"]), data=json.dumps(met_response).encode("utf-8")),
                True,
            )

        return met_response

    def _fetch_issue_data(self, series_id: int, issue_number: str) -> GenericMetadata:
        # Have to search for an IssueList as Issue is only usable via ID
        issues_list_results = self.fetch_issues_in_series(str(series_id))

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
            return self._map_comic_issue_to_metadata(f_record)

        if f_record is not None:
            return self._fetch_issue_data_by_issue_id(f_record.id)
        return GenericMetadata()

    def _fetch_issue_data_by_issue_id(self, issue_id: str) -> GenericMetadata:
        cache_series_data: MetSeries | None = None
        cvc = ComicCacher(self.cache_folder, self.version)
        cached_issues_result = cvc.get_issue_info(int(issue_id), self.id)

        if cached_issues_result and cached_issues_result[1]:
            cache_issue_data = json.loads(cached_issues_result[0].data)
            cached_series_result = cvc.get_series_info(str(cached_issues_result[0].series_id), self.id)

            if cached_series_result is not None:
                cache_series_data = json.loads(cached_series_result[0].data)

            # Inject issue count
            if cache_series_data is not None and cache_series_data.get("issue_count") is not None:
                cache_issue_data["series"]["issue_count"] = cache_series_data["issue_count"]

            return self._map_comic_issue_to_metadata(cache_issue_data)

        issue_url = urljoin(self.api_url, f"issue/{issue_id}/")
        met_response: MetIssue = cast(MetIssue, self._get_metron_content(issue_url, {}))

        # Get cached series info
        cached_series_result = cvc.get_series_info(str(cached_issues_result[0].series_id), self.id)

        if cached_series_result is not None:
            # Inject issue count
            cache_series_data = json.loads(cached_series_result[0].data)
            if cache_series_data is not None and cache_series_data.get("issue_count") is not None:
                met_response["series"]["issue_count"] = cache_series_data["issue_count"]

        cvc.add_issues_info(
            self.id,
            [
                CCIssue(
                    id=str(met_response["id"]),
                    series_id=str(met_response["series"]["id"]),
                    data=json.dumps(met_response).encode("utf-8"),
                )
            ],
            True,
        )

        # Now, map the GenericMetadata data to generic metadata
        return self._map_comic_issue_to_metadata(met_response)

    def _fetch_series_cover(self, series_id: int, issue_count: int) -> str:
        # Metron/Mokkari does not return an image for the series therefore fetch the first issue cover
        url = urljoin(self.api_url, "issue/")
        met_response: MetResult[list[MetIssue]] = cast(
            MetResult[list[MetIssue]],
            self._get_metron_content(url, {"series_id": series_id}),
        )

        # Inject a series cover image
        img = ""
        # Take the first record, it should be issue 1 if the series starts at issue 1
        if len(met_response["results"]) > 0:
            img = met_response["results"][0]["image"]

        return img

    def _map_comic_issue_to_metadata(self, issue: MetIssue) -> GenericMetadata:
        series = issue["series"]
        # Cover both IssueList (with less data) and Issue
        md = GenericMetadata(
            data_origin=MetadataOrigin(self.id, self.name),
            issue_id=utils.xlate(issue["id"]),
            issue=utils.xlate(IssueString(issue["number"]).as_string()),
            series=utils.xlate(series["name"]),
        )

        if series.get("id"):
            md.series_id = utils.xlate(series["id"])

        if issue.get("cover_date"):
            if issue["cover_date"]:
                md.day, md.month, md.year = utils.parse_date_str(issue["cover_date"])
            elif series["year_began"]:
                md.year = utils.xlate_int(series["year_began"])

        md._cover_image = issue["image"]

        if issue.get("publisher"):
            md.publisher = utils.xlate(issue["publisher"].get("name"))
        if issue.get("imprint"):
            md.imprint = issue["imprint"].get("name")

        series_type = -1
        if series.get("series_type"):
            md.format = series["series_type"].get("name")

        # Check if series is ongoing to legitimise issue count OR use option setting
        if series.get("issue_count"):
            if series_type != MetronSeriesType.ongoing.value or self.use_ongoing_issue_count:
                md.issue_count = utils.xlate_int(series["issue_count"])

        md.description = issue.get("desc", None)

        if series.get("genres"):
            genres = []
            for genre in series["genres"]:
                genres.append(genre["name"])
            md.genres = set(genres)

        #  issue_name is only for IssueList, it's just the series name is issue number, we'll ignore it

        # If there is a collection_title (for TPB) there should be no story_titles?
        md.title = utils.xlate(issue.get("title", ""))

        if issue.get("name"):
            if len(issue["name"]) > 0:
                md.title = "; ".join(issue["name"])

        if issue.get("rating") and issue["rating"]["name"] != "Unknown":
            md.maturity_rating = issue["rating"]["name"]

        url = issue.get("resource_url", None)
        if url:
            try:
                md.web_links = [parse_url(url)]
            except LocationParseError:
                ...

        if issue.get("variants"):
            for alt in issue["variants"]:
                md._alternate_images.append(alt["image"])

        if issue.get("characters"):
            for character in issue["characters"]:
                md.characters.add(character["name"])

        if issue.get("teams"):
            for team in issue["teams"]:
                md.teams.add(team["name"])

        if issue.get("arcs"):
            for arc in issue["arcs"]:
                md.story_arcs.append(arc["name"])

        if issue.get("credits"):
            for person in issue["credits"]:
                person_name = person.get("creator", "")
                # A person is not required to have a role, metron returns an empty list
                roles = [role["name"] for role in person["role"]] if person["role"] else [""]
                for role in roles:
                    md.add_credit(person_name, role, False)

        md.volume = utils.xlate_int(series.get("volume"))
        if self.use_series_start_as_volume:
            md.volume = series.get("year_began")

        md.price = utils.xlate_float(issue.get("price", ""))

        return md

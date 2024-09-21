# Metron.cloud plugin for Comic Tagger

A plugin for [Comic Tagger](https://github.com/comictagger/comictagger/releases) to allow the use of the metadata from [Metron.cloud](https://metron.cloud/).

**NOTE:** Due to the bandwidth usage of cover matching, the auto-tagging features will no longer download the covers for comparison. Metron will be taking donations soon, so if you are able to contribute financially you will be able to. Check the website for details.

## Installation

The easiest installation method as of ComicTagger 1.6.0-alpha.23 for the plugin is to place the [release](https://github.com/mizaki/mangadex_talker/releases) zip file
`mangadex_talker-plugin-<version>.zip` (or wheel `.whl`) into the [plugins](https://github.com/comictagger/comictagger/wiki/Installing-plugins) directory.

## Development Installation

You can build the wheel with `tox run -m build` or clone ComicTagger and clone the talker and install the talker into the ComicTagger environment `pip install -e .`

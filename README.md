# Metron.cloud plugin for Comic Tagger

A plugin for [Comic Tagger](https://github.com/comictagger/comictagger/releases) to allow the use of the metadata from [Metron.cloud](https://metron.cloud/).

Please support Metron's costs and further development by [donating](https://opencollective.com/metron) if you are able, thank you.

**NOTE:** Due to the bandwidth usage of cover matching, the auto-tagging features will no longer download the covers for comparison.

## Installation

The easiest installation method as of ComicTagger 1.6.0-beta.1 for the plugin is to place the [release](https://github.com/comictagger/metron_talker/releases) zip file
`metron_talker-plugin-<version>.zip` into the [plugins](https://github.com/comictagger/comictagger/wiki/Installing-plugins) directory.

## Development Installation

You can build the wheel with `tox run -m build` or clone ComicTagger and clone the talker and install the talker into the ComicTagger environment `pip install -e .`

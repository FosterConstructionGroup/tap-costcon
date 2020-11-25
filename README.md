# tap-costcon

This is a [Singer](https://singer.io) tap that produces JSON-formatted
data from Costcon exports following the [Singer
spec](https://github.com/singer-io/getting-started/blob/master/SPEC.md).

This tap:

- Parses Costcon exports
- Outputs the schema for each resource

## Quick start

1. Install

   We recommend using a virtualenv:

   ```bash
   > virtualenv -p python3 venv
   > source venv/bin/activate
   > pip install -e .
   ```

2. Create the config file

   Create a JSON file called `config.json` containing the path to the folder with the exports.

   ```json
   {
     "folder": "path/to/folder"
   }
   ```

3. Run the tap in discovery mode to get properties.json file

   ```bash
   tap-costcon --config config.json --discover > properties.json
   ```

4. In the properties.json file, select the streams to sync

   Each stream in the properties.json file has a "schema" entry. To select a stream to sync, add `"selected": true` to that stream's "schema" entry. For example, to sync the `jobs` stream:

   ```
   ...
   "tap_stream_id": "jobs",
   "schema": {
     "selected": true,
     "properties": {
   ...
   ```

5. Run the application

   `tap-costcon` can be run with:

   ```bash
   tap-costcon --config config.json --properties properties.json
   ```

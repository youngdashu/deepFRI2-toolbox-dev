import json
from bs4 import BeautifulSoup
import subprocess

DEFAULT_FILE_PATH = '/Users/youngdashu/sano/deepFRI2-toolbox-dev/experiments/download/report_pdb_8_10.html'


def parse_html_to_json(html_file_path: str, parser='html.parser'):
    with open(html_file_path, 'r') as file:
        html_content = file.read()
    soup = BeautifulSoup(html_content, parser)
    return soup.find('script', type='application/json')


def convert_json_to_schema(json_script: BeautifulSoup):
    if json_script:
        json_data = json.loads(json_script.string)
        return json_data


def generate_schema(json_data: dict):
    gen_schema_proc = subprocess.Popen(["genson"], stdin=subprocess.PIPE, stdout=subprocess.PIPE, text=True)
    json_str = json.dumps(json_data)
    schema_str, _ = gen_schema_proc.communicate(json_str)
    return schema_str


def organise_json(json_data: dict):
    jq_proc = subprocess.Popen(["jq", "."], stdin=subprocess.PIPE, stdout=subprocess.PIPE, text=True)
    organized_json_str, _ = jq_proc.communicate(json.dumps(json_data))
    return organized_json_str


def save_to_file(data: str, filename='pretty.json'):
    with open(filename, 'w') as f:
        f.write(data)


def main_json():
    html_file_path = DEFAULT_FILE_PATH
    json_script = parse_html_to_json(html_file_path)
    json_data = convert_json_to_schema(json_script)
    if json_data is not None:
        organized_json_str = organise_json(json_data)
        save_to_file(organized_json_str, filename='pretty.json')


if __name__ == '__main__':
    main_json()

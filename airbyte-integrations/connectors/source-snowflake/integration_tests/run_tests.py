import subprocess
import json
from itertools import product
import os
from tqdm import tqdm

"""
To use this script correctly to run tests for local non regression:
 0- Set up your sql environment with scripts in integration_tests/sql_set_up (In progress)
 1- Fill the private config file example_private_config.json with your private information and put the file in a path locally
 2- Set the env var CONFIG_SNOWFLAKE with the value: path/example_private_config.json
 3- run python run_tests.py from the folder integration_tests

Important Note: 
You can custom your tests by altering the file in configs depending on the scenario you want to test
"""

# Define the base directory for the configuration files
base_dir = "configs"

# Define the categories and their possible values
# modes = ['full_refresh', 'incremental']
# types = ['standard', 'history']
# streams = ['full_stream', 'push_down_filter_stream']

modes = ['full_refresh', 'incremental']
types = ['standard', 'history']
streams = ['full_stream', 'push_down_filter_stream']
WORK_DIR = os.path.dirname(os.getcwd())
# This file contains data that is common for configs but can't be shared in the git (private key, warehouse etc...)
try:
    CONFIG_SNOWFLAKE = os.environ['CONFIG_SNOWFLAKE']
except KeyError:
    raise KeyError('Set the env variable CONFIG_SNOWFLAKE to point to the path of your config.json that contains private parameters')


# Function to run bash command and return the result
def run_bash_command(command):
    try:
        result = subprocess.run(command, shell=True, check=True, capture_output=True, text=True, cwd=WORK_DIR)
        return result.returncode, result.stdout
    except subprocess.CalledProcessError as e:
        return e.returncode, e.output


# Function to load JSON files
def load_json(file_path):
    with open(file_path) as f:
        return json.load(f)

def enrich_config(file_path, basic_config):

    with open(CONFIG_SNOWFLAKE) as f:
        additional_configuration = json.load(f)

    enriched_config = basic_config | additional_configuration

    with open(file_path, 'w') as json_file:
        json.dump(enriched_config, json_file, indent=4)


def reset_config(file_path, basic_config):
    with open(CONFIG_SNOWFLAKE) as f:
        additional_configuration = json.load(f)

    # in case there is a crash and the file is not reset
    basic_config = {k: v for k, v in basic_config.items() if k not in additional_configuration}

    with open(file_path, 'w') as json_file:
        json.dump(basic_config, json_file, indent=4)


def run_test_scenario(commands, mode, type_, stream, config_file, catalog_file):
    scenario_results = []
    scenario_failed_commands = []
    print('Scenario: ', mode, type_, stream)
    # Run each command and capture the result
    for cmd_template in tqdm(commands):
        command = cmd_template.format(config_file=config_file, catalog_file=catalog_file)
        return_code, output = run_bash_command(command)

        # Check if the process finished properly (return_code == 0)
        status = "------PASSED------" if return_code == 0 else "*****FAILED*****"

        # Log the result
        scenario_results.append({
            "mode": mode,
            "type": type_,
            "stream": stream,
            "command": command,
            "status": status
        })

        # If the command went wrong, log it in the failed commands list
        if return_code == 1:
            scenario_failed_commands.append({
                "mode": mode,
                "type": type_,
                "stream": stream,
                "command": command
            })
    return scenario_results, scenario_failed_commands


def print_report(results):
    # Generate a report
    report = "Test Report:\n"
    current_scenario = None
    for result in results:
        scenario = (result['mode'], result['type'], result['stream'])
        if scenario != current_scenario:
            if current_scenario is not None:
                report += "\n" + "-" * 40 + "\n"
            current_scenario = scenario
            report += f"Mode: {result['mode']}, Type: {result['type']}, Stream: {result['stream']}\n"

        if 'check' in result['command']:
            command_type = 'check'
        if 'discover' in result['command']:
            command_type = 'discover'
        if 'read' in result['command']:
            command_type = 'read'

        report += f"  {command_type} - Status: {result['status']} \n Command: {result['command']} \n"

    # Print the report
    print(report)

def print_failed_commands(failed_commands):
    # Print all the commands that went wrong
    if failed_commands:
        print("\nCommands that went wrong:")
        for failed_command in failed_commands:
            print(f"Mode: {failed_command['mode']}, Type: {failed_command['type']}, Stream: {failed_command['stream']}")
            print(f"  Command: {failed_command['command']}")
            print("-" * 40)
    else:
        print("All commands went well!")



def main():
    # Iterate over all combinations of configurations
    results = []
    failed_commands = []

    commands = [
        "poetry run source-snowflake check --config integration_tests/{config_file}",
        "poetry run source-snowflake discover --config integration_tests/{config_file}",
        "poetry run source-snowflake read --config integration_tests/{config_file} --catalog integration_tests/{catalog_file}"
    ]

    for mode, type_, stream in product(modes, types, streams):
        # Construct the path to the configuration files
        config_dir = os.path.join(base_dir, mode, type_, stream)
        config_file = os.path.join(config_dir, "config.json")
        catalog_file = os.path.join(config_dir, "catalog.json")

        basic_config = load_json(config_file)
        enrich_config(config_file, basic_config)

        scenario_results, scenario_failed_commands = run_test_scenario(commands, mode, type_, stream, config_file, catalog_file)
        results += scenario_results
        failed_commands += scenario_failed_commands
        if not len(scenario_failed_commands):
            reset_config(config_file, basic_config)

    print_report(results)
    print_failed_commands(failed_commands)


if __name__ == '__main__':
    main()

""" Helper utility to return the path to the latest valid autobuild.
The stdout from normal run should be only the path of the valid build.
"""

from datetime import datetime, timedelta
import json
import itertools
import logging
import os
#import pprint
import subprocess

from BeautifulSoup import BeautifulSoup

FORMAT = '%(message)s'
logging.basicConfig(level=logging.ERROR, format=FORMAT)
logger = logging.getLogger(__name__)

AUTOBUILD_PATH = (
    r"Y:\rev7x\auto_builds\HQ-ENG4-2008R2E-builds",
    r"Y:\rev7x\auto_builds\SWAUTOBUILD4-builds",
)

FOLDER_MATCH = ['g7.0_', 'A70_', 'emake_g7.0_']


class Error(Exception):
    """ all errors thrown by this module should inherit from this class """
    pass


class NodesFailedToBuild(Error):
    """ found nodes that failed to build """
    pass


class NodeTimestampOutOfRange(Error):
    """ found a timestamp on a node build that is significantly different from
    timestamps on others """
    pass


class DataFailedToBuild(Error):
    """ data cfg failed to build """
    pass


class LocalModificationsFound(Error):
    """ tree has local modifications """
    pass


class NullVersionsFound(Error):
    """ found zeroed versions in Embedded_versions.txt """
    pass


def best_folder_candidate(top_level):
    " generator for the best candidate for one particular top level folder "

    try:
        #arbitrary order
        dir_list = os.listdir(top_level)

        #remove folders that do not match autobuild pattern
        dirs = []
        for entry, pattern in itertools.product(dir_list, FOLDER_MATCH):
            if not entry.startswith(pattern):
                continue

            #add only folders
            abs_path = os.path.join(top_level, entry)
            if os.path.isdir(abs_path):
                dirs.append(abs_path)

        #newest first
        dirs.sort(key=lambda x: os.stat(x).st_mtime, reverse=True)

        #check for pattern match
        for candidate in dirs:
            yield candidate

    except WindowsError as err:
        logger.exception(err)


def best_folder_candidates(autobuild_path=AUTOBUILD_PATH):
    """ generator for best candidates among all given folders """

    #instantiate all top level folders
    generators = []
    for top_level in autobuild_path:
        generators.append(best_folder_candidate(top_level))

    #cycle through them all one by one, and choose best candidate
    for generator in itertools.cycle(generators):
        yield generator.next()


def validate_json_build(candidate_path):
    """ validate that all nodes are built """

    logger.info("validating node build status...")

    json_filepath = os.path.join(candidate_path, 'build_status.json')

    start_build_timestamp = os.path.getctime(json_filepath)
    start_build_time = datetime.fromtimestamp(start_build_timestamp)

    with open(json_filepath) as file_handle:
        json_bd = json.loads(file_handle.read())

        failed_to_build = []

        #check status
        for node in json_bd:
            status = json_bd[node]['status']
            timestamp = datetime.fromtimestamp(json_bd[node]['epochsecs'])

            if status != 'OK':
                failed_to_build.append(node)

            build_delta = timestamp - start_build_time

            if  build_delta > timedelta(minutes=60):
                raise NodeTimestampOutOfRange(
                    "Node %s build looks stale (%s, and build started @ %s)" %
                    (node, timestamp, start_build_time))

        if failed_to_build:
            failed_str = ', '.join(failed_to_build)
            err_msg = 'Nodes {} failed to build'.format(failed_str)
            raise NodesFailedToBuild(err_msg)


def validate_data_build_status(candidate_path):
    """ Inspect data build status from html """

    logger.info("validating data build status...")

    status_filepath = os.path.join(candidate_path, 'all_build_status.html')

    html = open(status_filepath, 'r')
    soup = BeautifulSoup(html)

    #analytics starts here
    data_db = {}

    header = soup.find('h1')

    #pp = pprint.PrettyPrinter(indent=4)

    while(header):
        if header.text == 'Summary Status':
            pass

        if header.text == 'Data Build Status':
            name_token = header.findNext('td')
            data_name = name_token.text
            status_token = name_token.findNextSibling('td')
            status = status_token.text
            link_token = status_token.findNextSibling('td')
            log_text = link_token.findNext('a').text
            data_db[data_name] = {'status': status, 'log': log_text}
            logger.debug('extracted %s info', data_name)

            name_token = link_token.findNext('td')
            data_name = name_token.text
            status_token = name_token.findNextSibling('td')
            status = status_token.text
            link_token = status_token.findNextSibling('td')
            log_text = link_token.findNext('a').text
            data_db[data_name] = {'status': status, 'log': log_text}
            logger.debug('extracted %s info', data_name)

        if header.text == 'MatlabSys Compiled Status':
            pass

        if header.text == 'Build Status':
            break

        header = header.findNext('h1')

    #pp.pprint(data_db)
    found_len = len(data_db)
    logger.debug("extracted build information about %s cfg data", found_len)
    if found_len != 2:
        err_msg = "Invalid number of data_db records found (%s!=2)" % found_len
        raise DataFailedToBuild(err_msg)

    for node in data_db:
        if data_db[node]['status'] != 'OK':
            raise DataFailedToBuild("Data node %s did not build." % node)


def check_versions(candidate_path):
    """ make sure all versions are set properly in EMBEDDED_Versions.txt """

    logger.info("checking embedded versions...")

    versions_filepath = os.path.join(candidate_path,
                                     'EMBEDDED',
                                     'Targets',
                                     'EMBEDDED_Versions.txt')

    with open(versions_filepath, 'r') as file_handle:
        #normally, there is no '00.00.00.00' in Embedded_Versions.txt except the line after "ITEMS_BELOW_HERE_NOT_SW_RELEASED"
        for line in file_handle:
            if line.startswith("ITEMS_BELOW_HERE_NOT_SW_RELEASED"):
                break
            if '00.00.00.00' in line and not (line.startswith('PTPC_ARM') or line.startswith('TPC_ARM')):
            	
                err_msg = ("'00.00.00.00' found in %s" % versions_filepath)
                raise NullVersionsFound(err_msg)


def check_local_mods(candidate_path):
    """ make sure there are no local modifications """

    logger.info("checking local modifications...")

    #this also captures missing files if copy is in progress
    batch_cmd = (r'V:\sys\tools\Subversion1.8.9\bin\svn status '
                 r'-q --ignore-externals | findstr "^\! ^M"')

    #TODO: string search should be done in python to handle errors in SVN, 
    # OleksiyP 02/28/2014
    try:
        cmd_output = subprocess.check_output(batch_cmd, cwd=candidate_path,
                                             shell=True)
    except subprocess.CalledProcessError as err:
        #findstr should return exit status 1 (match not found)
        if err.returncode == 1:
            return

    raise LocalModificationsFound("%s has local modifications (%s)" %
                                  (candidate_path, cmd_output[:100]))


def grouper(iterable, num, fillvalue=None):
    "Collect data into fixed-length chunks or blocks"
    # grouper('ABCDEFG', 3, 'x') --> ABC DEF Gxx
    args = [iter(iterable)] * num
    return itertools.izip_longest(fillvalue=fillvalue, *args)


def best_candidate():
    """ top level function to pick latest solid autobuild """

    # Process in batches sized len(AUTOBUILD_PATH).
    # We are taking one best candidate from each folder, sort it by time
    # and process with newest one first
    for candidates in grouper(best_folder_candidates(), 10):

        candidates_list = list(candidates)
        candidates_list.sort(key=lambda x: os.stat(x).st_mtime, reverse=True)

        for candidate_path in candidates_list:
            try:
                logger.info("Trying %s", candidate_path)
                validate_json_build(candidate_path)
                validate_data_build_status(candidate_path)
                check_versions(candidate_path)
                check_local_mods(candidate_path)
            except (Error, IOError, WindowsError) as err:
                logger.exception(err)
                continue

            logger.info("%s is good!", candidate_path)
            return candidate_path

if __name__ == '__main__':
    print best_candidate()

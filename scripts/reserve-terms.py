#!/usr/bin/env python3

import argparse
import base64
import json
import logging
import os
import re
import requests
import sys
import textwrap

from urllib.parse import urlencode


GITHUB_API_ACCESS_TOKEN = os.getenv('GITHUB_API_ACCESS_TOKEN')
if not GITHUB_API_ACCESS_TOKEN:
  raise Exception("Could not determine GITHUB_API_ACCESS_TOKEN")

GITHUB_API_URL = 'https://api.github.com'
GITHUB_API_DEFAULT_HEADERS = {'Authorization': 'token ' + GITHUB_API_ACCESS_TOKEN,
                              'Accept': 'application/vnd.github.v3+json'}

GITHUB_REPO = 'gizmos'
GITHUB_OWNER = 'ontodev'
GITHUB_BRANCH = 'term-ids'
PUBLISHED_FILE = 'published-terms.txt'
RESERVED_FILE = 'reserved-terms.txt'

# Initialize the logger:
logging.basicConfig(format='%(asctime)-15s %(name)s %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)
logger.setLevel(logging.WARN)


def github_get(endpoint, parameters={}):
  """
  Make a GET request to the Github API for the given endpoint with the given parameters
  """
  endpoint = endpoint.strip('/')
  query_string = "?" + urlencode(parameters) if parameters else ""
  url = "{}/{}{}".format(GITHUB_API_URL, endpoint, query_string)

  logger.debug("Sending GET request: {}".format(url))
  response = requests.get(url, headers=GITHUB_API_DEFAULT_HEADERS)
  if response.status_code != requests.codes.ok:
    response.raise_for_status()
  return response.json()


def github_post(endpoint, data):
  """
  Make a POST request to the Github API for the given endpoint with the given data
  """
  endpoint = endpoint.strip('/')
  url = "{}/{}".format(GITHUB_API_URL, endpoint)

  try:
    data = json.dumps(data)
  except Exception:
    logger.error("Unable to convert {} to JSON.")
    sys.exit(1)

  logger.debug("Sending POST request: {} with data: {}".format(url, data))
  response = requests.post(url, headers=GITHUB_API_DEFAULT_HEADERS, data=data)
  if response.status_code != requests.codes.ok:
    response.raise_for_status()
  return response.json()


def github_put(endpoint, data):
  """
  Make a PUT request to the Github API for the given endpoint with the given data
  """
  endpoint = endpoint.strip('/')
  url = "{}/{}".format(GITHUB_API_URL, endpoint)

  try:
    data = json.dumps(data)
  except Exception:
    logger.error("Unable to convert {} to JSON.")
    sys.exit(1)

  logger.debug("Sending PUT request: {} with data: {}".format(url, data))
  response = requests.put(url, headers=GITHUB_API_DEFAULT_HEADERS, data=data)
  if response.status_code != requests.codes.ok:
    response.raise_for_status()
  return response.json()


def get_term_files_content():
  """
  For the lists of published and reserved commits stored in github, return their contents and
  the SHA used by github to identify their respective files.
  """
  logger.info("Retrieving currently published and reserved terms ...")

  info_to_return = {}
  for filename in [PUBLISHED_FILE, RESERVED_FILE]:
    response = github_get("/repos/{owner}/{repo}/contents/{path}"
                          .format(owner=GITHUB_OWNER, repo=GITHUB_REPO, path=filename),
                          {'ref': GITHUB_BRANCH})
    if 'sha' not in response:
      raise Exception("Could not determine SHA for {}".format(filename))
    if 'content' not in response:
      raise Exception("No file content found for {}".format(filename))

    decodedBytes = base64.b64decode(response['content'])
    info_to_return[filename] = {'sha': response['sha'],
                                'content': str(decodedBytes, "utf-8").strip('\n')}
  return info_to_return


def commit_reserved(content, commit_msg, sha):
  """
  Given new content to save to the reserved terms list, a commit message, and the SHA that github
  uses to identify the file for the reserved terms list, create a commit and then return the URL
  for that commit.
  """
  logger.info("Committing to {}/{}/{}".format(GITHUB_OWNER, GITHUB_REPO, RESERVED_FILE))
  response = github_put('/repos/{}/{}/contents/{}'.format(GITHUB_OWNER, GITHUB_REPO, RESERVED_FILE),
                        {'message': commit_msg,
                         'content': base64.b64encode(content.encode("utf-8")).decode(),
                         'branch': GITHUB_BRANCH,
                         'sha': sha})

  if 'commit' not in response or 'html_url' not in response['commit']:
    logger.error("Unable to extract 'html_url' from successful response.")
    logger.debug("Response was: {}".format(response))
    return None
  return response['commit']['html_url']


def get_next_ontology_id(terms_to_add, current_terms):
  """
  Given a list of current terms, return the next unique ontology id to use for subsequent term
  additions, while verifying that none of the given terms to be added already exist.
  """
  used_ids = []
  used_terms = {}
  for filename in [PUBLISHED_FILE, RESERVED_FILE]:
    used_terms[filename] = []
    for line in current_terms[filename]['content'].splitlines():
      line = line.strip()
      matched = re.match(r"^OBI:(\d+)\s+(.+)", line)
      if not matched:
        logger.warning("Ignoring line: '{}' in {} that could not be parsed."
                       .format(line, filename))
      else:
        used_ids.append(int(matched[1]))
        used_term = matched[2]
        if used_term in terms_to_add:
          logger.error("Proposed new term: '{}' already exists in {}. Exiting."
                       .format(used_term, filename))
          sys.exit(1)
        used_terms[filename].append(used_term)

  return (sorted(used_ids).pop() + 1) if used_ids else 1


def prepare_new_reserved_term_content(current_reserved_content, terms_to_add, next_id):
  """
  Append the given terms to add to the content of the current reserved list of terms, using ids
  beginning at the given next_id, and return the new list.
  """
  new_reserved_term_content = current_reserved_content
  for i in range(0, len(terms_to_add)):
    next_line = "OBI:{} {}".format(str(next_id + i).zfill(7), terms_to_add[i])
    print("Adding {}".format(next_line))
    if new_reserved_term_content != "":
      new_reserved_term_content += "\n"
    new_reserved_term_content += next_line

  return new_reserved_term_content


def main():
  description = textwrap.dedent('''
  Read a number of terms either from the command line or from a file (containing one term per line)
  and add them to the list of reserved terms in {}/{}/{} on the branch {}, checking first to verify
  that none of the terms are either published (in {}) or already reserved. If no commit message is
  specified, the user will be prompted to supply one.'''.format(
    GITHUB_OWNER, GITHUB_REPO, RESERVED_FILE, GITHUB_BRANCH, PUBLISHED_FILE))
  parser = argparse.ArgumentParser(description=description)

  parser.add_argument('--commit_message', metavar='MESSAGE',
                      help=('The message describing the commit in Github. It should include a '
                            'comment with a GitHub issue or PR number (e.g. #1234).'))

  term_args = parser.add_mutually_exclusive_group(required=True)
  term_args.add_argument('--terms', metavar='TERM', nargs='+',
                         help=('A list of terms to add, separated by spaces. If a term contains '
                               'spaces it should be surounded by single or double quotes'))

  term_args.add_argument('--input', type=argparse.FileType('r'),
                         help='A file containing a list of terms to add, one per line')
  args = vars(parser.parse_args())

  if args.get('input'):
    # Ignore any empty lines.
    terms_to_add = [l.strip() for l in args.get('input').readlines() if l.strip() != ""]
  else:
    terms_to_add = args.get('terms')

  # This might happen if the terms are given through an input file and it is empty:
  if not terms_to_add:
    logger.error("No terms specified.")
    sys.exit(1)

  # Prompt the user if no commit message was supplied:
  commit_msg = args.get('commit_message')
  if not commit_msg or not commit_msg.strip():
    try:
      commit_msg = input("Please enter a commit message: ").strip()
      if not commit_msg:
        print("A commit message is required.")
        sys.exit(1)
    except KeyboardInterrupt:
      sys.exit(1)

  # Retrieve the currently published and reserved terms:
  current_terms = get_term_files_content()
  # Determine the next id to use based on the current list:
  next_id = get_next_ontology_id(terms_to_add, current_terms)
  # Prepare the contents of the file listing reserved commits (including the new ones):
  new_reserved_term_content = prepare_new_reserved_term_content(
    current_terms[RESERVED_FILE]['content'], terms_to_add, next_id)
  # Commit the file and inform the user where (s)he can view the commit contents:
  url = commit_reserved(new_reserved_term_content, commit_msg, current_terms[RESERVED_FILE]['sha'])
  print("Commit successful. You can review it on github at: {}".format(url))


if __name__ == '__main__':
  main()

__author__ = 'Tirth Patel <complaints@tirthpatel.com>'

import re
import os
import sys
import time

import requests as req


BASE_URL = 'http://www.ncbi.nlm.nih.gov/blast/Blast.cgi?'


def search_url(**kwargs):
    args = ['{}={}'.format(parameter.upper(), value)
            for parameter, value in kwargs.items()]

    return BASE_URL + 'CMD=Put&' + '&'.join(args)


def result_url(request_id, **kwargs):
    args = ['{}={}'.format(parameter.upper(), value)
            for parameter, value in kwargs.items()]

    return BASE_URL + 'CMD=Get&RID=' + request_id + ('&' + '&'.join(args) if args else '')


def submit_query(query, database='nr', program='blastn', filter='L;m;', expect='10',
                 format_type='HTML', hitlist_size='100', word_size='11'):

    r = req.get(search_url(query=query, database=database, program=program, filter=filter, expect=expect,
                           format_type=format_type, hitlist_size=hitlist_size, word_size=word_size,
                           email='complaints@tirthpatel.com'))

    if r.status_code == 200:
        result_section = re.findall(r'QBlastInfoBegin([\s|\S]*)QBlastInfoEnd', r.text)

        if result_section:
            result = result_section[0]
        else:
            raise UnsuccessfulQueryError('Server error, possibly invalid query sequence')

        request_id = re.findall(r'RID = (\w+)\s', result)[0]
        eta = re.findall(r'RTOE = (\d+)\s', result)[0]

        if not request_id:
            error = re.findall(r'<div class="msg error">(\w+)</div>', result)[0]
            raise UnsuccessfulQueryError('BLAST error: ' + error)

        return request_id, eta
    else:
        raise UnsuccessfulQueryError('HTTP request error: ' + r.status_code)


def retrieve_results(request_id):
    r = req.get(result_url(request_id, format_type='Text'))

    if r.status_code == 200:
        result_section = re.findall(r'QBlastInfoBegin([\s|\S]*)QBlastInfoEnd', r.text)

        if result_section:
            result = result_section[0]
        else:
            raise UnsuccessfulQueryError('Retrieval server error')

        status = re.findall(r'Status=(\w+)\s', result)[0]

        if status == 'READY':
            os.makedirs('results', exist_ok=True)
            with open('results\\' + request_id + '.txt', 'w') as output:
                output.write(r.text)
        else:
            raise UnsuccessfulQueryError('BLAST not ready')
    else:
        raise UnsuccessfulQueryError('HTTP request error: ' + r.status_code)


class UnsuccessfulQueryError(Exception):
    pass


def run_queries(queries, delay=3):
    submitted_queries = []
    max_eta = 0

    for idx, query in enumerate(queries):
        print('submitting query #' + str(idx + 1), end='... ')

        try:
            rid, eta = submit_query(query)
            print(rid, 'ETA:', eta, 'seconds')

            submitted_queries.append(rid)
            max_eta = max(max_eta, int(eta))

        except UnsuccessfulQueryError as e:
            print(e.args[0])

        time.sleep(delay)
    print()

    return max_eta, submitted_queries


def retrieve_queries(submitted_queries, delay=3):
    retrieved = []

    for query in submitted_queries:
        print('Retrieving query', query, end='... ')

        try:
            retrieve_results(query)
            retrieved.append(query)
            print('done!')
        except UnsuccessfulQueryError as e:
            print(e.args[0])

        time.sleep(delay)
    print()

    return [q for q in submitted_queries if q not in retrieved]


def wait_for(seconds, step=5):
    delay = int(seconds)

    print('---Longest estimated time:', delay, 'seconds')

    for current in range(0, delay, step):
        print(delay - current, 'seconds left')
        time.sleep(step)

    print('---Done waiting\n')


def get_queries(filename):
    queries = []

    try:
        with open(filename + '.txt') as in_stuff:
            for line in in_stuff:
                if len(line) > 100:
                    queries.append(line.strip())
    except FileNotFoundError:
        print(filename, 'not found')
        sys.exit()

    return queries


def main(argv):
    if len(argv) != 1:
        print('supply one and only one filename')
        sys.exit()
    filename = argv[0]

    queries = get_queries(filename)
    eta, submitted_queries = run_queries(queries)

    print('Successfully submitted', len(submitted_queries), 'queries')
    wait_for(eta)

    missing = retrieve_queries(submitted_queries)

    while True:
        if missing:
            again = input(str(len(missing)) + ' queries could not be retrieved, try again? (y/n) ')
            if again.lower() == 'y':
                missing = retrieve_queries(missing)
            else:
                break
        else:
            break


if __name__ == '__main__':
    main(sys.argv[1:])
import os

from SuperJob.datalib.parsers import SuperjobParser


if __name__ == '__main__':

    # get data
    params = {}

    parser = SuperjobParser()
    df = parser.parse_vacancies(params)
    parser.save_result(
        df,
        os.path.join('results', 'parsed_data')        
    )
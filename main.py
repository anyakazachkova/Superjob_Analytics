from datalib.parsers import SuperjobParser


if __name__ == '__main__':

    # get data
    params = {'keywords': 'Python'}

    parser = SuperjobParser()
    df = parser.parse_vacancies(params)
    parser.save_result(
        df,
        'results',
        'parsed_data'
    )
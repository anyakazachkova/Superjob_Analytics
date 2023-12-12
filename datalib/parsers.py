import sys
import os
import requests
import logging
from datetime import timedelta, datetime
import pandas as pd
import dask.dataframe as dd
import numpy as np
from bs4 import BeautifulSoup
from typing import List, Dict, Any


class SuperjobParser:
    """
    Данный класс осуществлеет выгрузку вакансий с сайта Superjob 
    и сохраняет локально данные.
    """

    def __init__(self, 
                 chunksize: int = 100000
                 ) -> None:
        
        self.chunksize = chunksize
        
        self._get_logger()

    def _get_logger(self):
        self.logger = logging.Logger('logger')
        handler = logging.StreamHandler(stream=sys.stdout)
        self.logger.addHandler(handler)

    def _parse_search_page(self, 
                           params: Dict[str, Any] = dict()
                           ) -> Dict[str, str]:

        hrefs, names = [], []

        page = 1
        while True:
            self.logger.info(f'Start parsing vacancies: {params}: page {page}')
            response = requests.get(
                'https://www.superjob.ru/vacancy/search/', 
                params={
                    **params,
                    'page': page
                }
            )

            if response.status_code in [200, 201, 202]:
                tree = BeautifulSoup(response.content, 'html.parser')
                items = tree.find_all(
                                    'span', 
                                    {
                                        'class': '_3xQyu _3h-Il Ev2_p _3vg36 _133uk rPK4q _2ASNn bb-JF'
                                    }
                                )

                if len(items) == 0:
                    self.logger.info(f'No vacancies have been found on page {page}, stop searching')
                    break
                
                self.logger.info(f'Success: {len(items)} vacancies have been found')

                for item in items:
                    hrefs.append(item.a.get('href'))
                    names.append(item.a.text)
                
                page += 1
            
            else:
                self.logger.info(f'Parsing error: {response.text}')

        self.vacancies_list = zip(names, hrefs)

        return self.vacancies_list

    def _parse_vacancy_page(self,
                            href: str, 
                            name: str, 
                            params: Dict[str, Any] = dict()
                            ) -> Dict[str, Any]:
        response = requests.get(
            f'https://www.superjob.ru/{href}', 
            params={
                **params,
            }
        )

        vacancy_info = {
            'name': name,
            'url': f'https://www.superjob.ru/{href}'
        }

        if response.status_code in [200, 201, 202]:
            tree = BeautifulSoup(response.content, 'html.parser')

            # get salary
            try:
                salary = tree.find_all(
                    'span', 
                    {
                        'class': '_2eYAG _133uk rPK4q Mq4Ti'
                    }
                )[0].text
                vacancy_info['salary'] = ''.join(filter(str.isdigit, salary))
            except Exception as e:
                self.logger.exception(
                    f'Error while parsing salary in {href}: {e}'
                )

            # get education requirement
            try:
                data_found = tree.find_all(
                    'span',
                    {
                        'class': '_38__N rPK4q Mq4Ti'
                    }
                )
                vacancy_info['education'] = data_found[0].text.capitalize() \
                                                if len(data_found) > 0 else 'None'
            except Exception as e:
                self.logger.exception(
                    f'Error while parsing education requirement in {href}: {e}'
                )

            # get address, experience, employment
            try:
                vacancy_info['address'] = tree.find_all(
                    'span',
                    {
                        'class': '_1qYY4'
                    }
                )[0].text
            except Exception as e:
                self.logger.exception(
                    f'Error while parsing education address, experience, employment in {href}: {e}'
                )

            # get other info
            try:
                other_info = tree.find_all(
                    'span',
                    {
                        'class': '_38__N rPK4q Mq4Ti'
                    }
                )
                vacancy_info['experience'], \
                    vacancy_info['employment'] =  \
                            other_info[-2].text, other_info[-1].text.capitalize()
            except Exception as e:
                self.logger.exception(
                    f'Error while parsing other info in {href}: {e}'
                )
            
            # get description
            try:
                vacancy_info['description'] = tree.find_all(
                    'span',
                    {
                        'class': '_39I1Z _2u6Iv rPK4q _2ASNn Mq4Ti MFNgx'
                    }
                )[0].text
            except Exception as e:
                self.logger.exception(
                    f'Error while parsing description in {href}: {e}'
                )

            self.logger.info(f'Successfully parsed: {href}')
        
            return vacancy_info
        
        else:
            self.logger.info(f'Error while parsing vacancy {href}: {response.text}')
            raise ValueError(response.text)

    def parse_vacancies(self, 
                        params: Dict[str, Any] = dict()
                        ):
        vacancies_list = self._parse_search_page(params)

        result = []
        for key, value in vacancies_list:
            try:
                vacancy_info = self._parse_vacancy_page(value, key)
                result.append(vacancy_info)
            except Exception as e:
                self.logger.error(f'Error while parsing vacancy {value}: {e}')
        
        return pd.DataFrame(result)
    
    def save_result(self, 
                    df: pd.DataFrame, 
                    path: str
                    ) -> None:
        os.makedirs(path, exist_ok=True)
        dd.from_pandas(df, chunksize=self.chunksize) \
            .to_parquet(path)
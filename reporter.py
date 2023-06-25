import csv
import logging

from pathlib import Path

from time_counter import time_counter


class Reporter:
    def __init__(self):
        self.logger = logging.getLogger(
            f'{self.__module__}.{self.__class__.__name__}'
        )

    @time_counter
    def __call__(self, report_dir, result_level, result_object):
        self.report_dir = Path(report_dir)

        self.logger.debug(
            f'Save level rows {len(result_level)} and {len(result_object)}'
        )

        path = self.report_dir / 'id_to_level.csv'
        self.logger.info(f'Saving level rows to {path}')
        self.save_to_file(path, result_level)

        path = self.report_dir / 'id_to_object.csv'
        self.logger.info(f'Saving object rows to {path}')
        self.save_to_file(path, result_object)

    def save_to_file(self, path, row_queue):
        row = row_queue[0]

        if not path.parent.exists():
            path.parent.mkdir(parents=True)

        with path.open(mode='w', encoding='utf-8', newline='\n') as fp:
            writer = csv.DictWriter(fp, fieldnames=row._fields, delimiter=';')

            writer.writeheader()

            writer.writerows(map(lambda row: row._asdict(), row_queue))

import datetime

from typing import List
from bq_dts import base_connector
from bq_dts import helpers

DATE_MIN_GREGORIAN = datetime.date(1582, 10, 15)
DATE_MAX_GREGORIAN = datetime.date(2199, 12, 31)
DATE_DAY_OF_WEEK_ABBR = ['mon', 'tue', 'wed', 'thu', 'fri', 'sat', 'sun']
DATE_MONTH_ABBR = ['jan', 'feb', 'mar', 'apr', 'may', 'jun', 'jul', 'aug', 'sep', 'oct', 'nov', 'dec']

TIME_SECONDS_IN_DAY = 60 * 60 * 24

INTEGER_MIN = 0
INTEGER_MAX = 999999


def _date_to_bq_date(current_date):
    return str(current_date.strftime('%Y-%m-%d'))


def datetime_to_date_dict(current_date):
    assert isinstance(current_date, datetime.date)

    bq_date = _date_to_bq_date(current_date)
    date_first_of = current_date.replace(day=1)
    iso_year, iso_week, iso_dow = current_date.isocalendar()
    dow_mon06 = current_date.weekday()

    out_dict = dict()
    out_dict['date'] = bq_date
    out_dict['date_str'] = str(bq_date.replace('-', ''))
    out_dict['date_int'] = int(bq_date.replace('-', ''))
    out_dict['date_dash'] = bq_date
    out_dict['date_slash'] = bq_date.replace('-', '/')

    out_dict['day'] = int(current_date.day)
    out_dict['day_abbr'] = DATE_DAY_OF_WEEK_ABBR[dow_mon06]

    out_dict['dow_mon17'] = iso_dow
    out_dict['dow_mon06'] = dow_mon06
    out_dict['dow_sun17'] = dow_sun17 = (iso_dow % 7) + 1
    out_dict['dow_sun06'] = dow_sun17 - 1

    out_dict['week_mon'] = int(current_date.strftime('%W'))
    out_dict['week_sun'] = int(current_date.strftime('%U'))
    out_dict['week_iso'] = int(iso_week)

    out_dict['month'] = int(current_date.month)
    out_dict['month_abbr'] = DATE_MONTH_ABBR[current_date.month - 1]
    out_dict['month_first_day'] = _date_to_bq_date(date_first_of)

    quarter = int(int(current_date.month - 1) / 3) + 1
    quarter_month = (quarter * 3) - 2
    quarter_date = date_first_of.replace(month=quarter_month)
    out_dict['quarter'] = quarter
    out_dict['quarter_first_day'] = _date_to_bq_date(quarter_date)

    half = int(int(current_date.month - 1) / 6) + 1
    half_month = (half * 6) - 5
    half_date = date_first_of.replace(month=half_month)
    out_dict['half'] = half
    out_dict['half_first_day'] = _date_to_bq_date(half_date)

    out_dict['year'] = int(current_date.year)
    out_dict['year_iso'] = iso_year
    out_dict['year_first_day'] = _date_to_bq_date(date_first_of.replace(month=1))
    return out_dict


def datetime_to_time_dict(current_time):
    assert isinstance(current_time, datetime.datetime)
    out_dict = dict()
    out_dict['time'] = str(current_time.strftime('%H:%M:%S'))
    out_dict['hour'] = int(current_time.hour)
    out_dict['minute'] = int(current_time.minute)
    out_dict['second'] = int(current_time.second)
    out_dict['second_in_day'] = (int(current_time.hour) * 3600) + (int(current_time.minute) * 60) + int(current_time.second)
    out_dict['hour_ampm'] = int(current_time.strftime('%I'))
    out_dict['is_pm'] = bool(current_time.strftime('%p') == 'PM')
    return out_dict


def number_to_dict(num):
    out_dict = dict()
    out_dict['num'] = int(num)
    return out_dict


def yyyymmdd_to_date(in_str):
    return datetime.datetime.strptime(in_str, '%Y-%m-%d').date()


class CalendarConnector(base_connector.BaseConnector):
    """
    # Dev/Test
    python example/calendar_connector.py --gcs-tmpdir gs://{gcs_bucket}/{blob_prefix}/ --transfer-run-yaml example/transfer_run.yaml example/imported_data_info.yaml

    # Prod
    python example/calendar_connector.py --gcs-tmpdir gs://{gcs_bucket}/{blob_prefix}/ --ps-subname bigquerydatatransfer.{datasource-id}.{location-id}.run --use-bq-dts example/imported_data_info.yaml
    """
    TRANSFER_RUN_REQUIRED_PARAMS = {'min_date', 'max_date', 'min_num', 'max_num'}
    TRANSFER_RUN_INTEGER_PARAMS = ['min_num', 'max_num']

    def validate_transfer_run_params(self, transfer_run_params):
        min_date = yyyymmdd_to_date(transfer_run_params['min_date'])
        max_date = yyyymmdd_to_date(transfer_run_params['max_date'])
        assert min_date <= max_date
        assert DATE_MIN_GREGORIAN <= min_date <= DATE_MAX_GREGORIAN
        assert DATE_MIN_GREGORIAN <= max_date <= DATE_MAX_GREGORIAN
        assert min_date <= max_date

        min_num = transfer_run_params['min_num']
        max_num = transfer_run_params['max_num']
        assert INTEGER_MIN <= min_num <= INTEGER_MAX
        assert INTEGER_MIN <= max_num <= INTEGER_MAX
        assert min_num <= max_num

        return dict(min_date=min_date, max_date=max_date, min_num=min_num, max_num=max_num)

    def stage_tables_locally(self, run_ctx: base_connector.ManagedTransferRun=None, local_prefix=None)\
            -> List[base_connector.TableContext]:
        """

        :param run_ctx:
        :param local_prefix:
        :return:
        """
        # Step 1 - Generate each of the tables
        date_table_ctx = self.generate_date(run_ctx, local_prefix)
        time_table_ctx = self.generate_time(run_ctx, local_prefix)
        integer_table_ctx = self.generate_integer(run_ctx, local_prefix)

        # Step 2 - Collect all the created TableContexts and return as a single list
        return [date_table_ctx, time_table_ctx, integer_table_ctx]


    # Use "date_greg" table configuration from imported_data_info config
    @base_connector.table_stager('date_greg')
    def generate_date(self, run_ctx, local_prefix):
        """
        Generate dates between min_date and max_date inclusive as passed via the TransferRun

        :return:
        """
        # Step 1 - Read parameters from the Transfer Run
        raw_params = run_ctx.transfer_run['params']
        min_date = raw_params['min_date']
        max_date = raw_params['max_date']

        # Step 2 - Setup local output paths
        output_uri = local_prefix.joinpath('date_greg', 'data.json.gz')
        output_uri.dirname().makedirs_p()

        # Step 3 - Instead of calling APIs, programmatically generate tables
        offset_one_day = datetime.timedelta(days=1)
        current_date = min_date
        with helpers.GzippedJSONWriter(output_uri) as output_file:
            while current_date <= max_date:
                output_file.write(datetime_to_date_dict(current_date))

                current_date += offset_one_day

        return [output_uri]

    # Use "time" table configuration from imported_data_info config
    @base_connector.table_stager('time')
    def generate_time(self, run_ctx, local_prefix):
        # Step 1 - Setup local output paths
        output_uri = local_prefix.joinpath('time', 'data.json.gz')
        output_uri.dirname().makedirs_p()

        offset_one_second = datetime.timedelta(seconds=1)

        # Step 2 - Instead of calling APIs, programmatically generate tables
        today = datetime.datetime.today()
        with helpers.GzippedJSONWriter(output_uri) as output_file:
            current_datetime = today.replace(hour=0, minute=0, second=0)
            for _ in range(TIME_SECONDS_IN_DAY):
                output_file.write(datetime_to_time_dict(current_datetime))

                current_datetime += offset_one_second

        return [output_uri]

    # Use "num_999999" configuration from imported_data_info config
    @base_connector.table_stager('num_999999')
    def generate_integer(self, run_ctx, local_prefix):
        # Step 1 - Read parameters from the Transfer Run
        raw_params = run_ctx.transfer_run['params']

        min_num = raw_params['min_num']
        max_num = raw_params['max_num']

        # Step 2 - Setup local output paths
        output_uri = local_prefix.joinpath('num_999999', 'data.json.gz')
        output_uri.dirname().makedirs_p()

        # Step 3 - Instead of calling APIs, programmatically generate tables
        with helpers.GzippedJSONWriter(output_uri) as output_file:
            for x in range(min_num, max_num + 1):
                output_file.write(number_to_dict(x))

        return [output_uri]


if __name__ == '__main__':
    import logging
    FORMAT = '%(asctime)-15s %(levelname)s %(message)s'
    logging.basicConfig(format=FORMAT, level=logging.INFO)

    cdp = CalendarConnector()
    cdp.run()

import pandas as pd
import numpy as np
import logging

class MapeCalculation:
    # Define the column mappings
    column_mappings = [
        ['forecast', 'backcast', 'settlement', 'forecast_abs_error', 'backcast_abs_error', 'settlement_abs','forecast_mape', 'backcast_mape'],
        ['forecast_gross', 'backcast_gross', 'usage_final_gross', 'forecast_gross_abs_error','backcast_gross_abs_error', 'usage_final_gross_abs', 'forecast_gross_mape', 'backcast_gross_mape'],
        ['forecast_net', 'backcast_net', 'usage_final_net', 'forecast_net_abs_error', 'backcast_net_abs_error', 'usage_final_net_abs', 'forecast_net_mape', 'backcast_net_mape']
    ]

    def __init__(self, raw: pd.DataFrame, log_file: str = 'main.log'):
        self.raw = raw
        self.zone = False   

        # Set up the logger
        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(logging.DEBUG)

        # Prevent duplicate handlers
        if not self.logger.handlers:
            # Stream handler for console logging
            stream_handler = logging.StreamHandler()
            stream_formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
            stream_handler.setFormatter(stream_formatter)
            self.logger.addHandler(stream_handler)

            # File handler for file logging
            file_handler = logging.FileHandler(log_file)
            file_formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
            file_handler.setFormatter(file_formatter)
            self.logger.addHandler(file_handler)

        self.logger.debug('MapeCalculation instance created')  


    def hourly_aggregation(self, zone=False) -> pd.DataFrame:
        raw = self.raw.copy()
        
        cols_hourly = [
            col for mapping in self.column_mappings
            for col in mapping
            if col in raw.columns
        ]

        if not cols_hourly:
            msg = 'No columns found in the dataframe, make sure to define mappings correctly'
            self.logger.error(f'{msg}/n{raw.columns}/n{raw.head()}')
            raise ValueError(msg)
        sum_cols_hourly = {col: 'sum' for col in cols_hourly}
        # dynamic variables based on the zone or profile level aggregation
        if zone == True:
            attributes = ['proxy_date', 'hour', 'zone']
        elif zone == False:
            attributes = ['proxy_date', 'hour']

        df = raw.copy()
        df['proxy_date'] = pd.to_datetime(df['proxy_date']).dt.date
        df['hour'] = df['hour'].astype(int)
        df = df.groupby(attributes).agg(sum_cols_hourly).reset_index()
        # Add columns for forecast ABS error and backcast ABS error for both gross and net
        for mapping in self.column_mappings:
            f, b, s, f_abs_e, b_abs_e, s_abs_e, f_m, b_m = mapping
            if all(col in df.columns for col in [f, b, s]):
                df[f_abs_e] = abs(df[f] - df[s])
                df[b_abs_e] = abs(df[b] - df[s])
                df[s_abs_e] = abs(df[s])
        df = df.sort_values(['proxy_date', 'hour'])
        self.logger.info(f'Hourly aggregation completed for {len(df)} rows')
        return df


    def daily_mape_aggregation(self, df_hourly: pd.DataFrame, zone=False) -> pd.DataFrame:
        
        cols_daily = [
            col for mapping in self.column_mappings
            for col in mapping
            if col in df_hourly.columns
        ]

        if not cols_daily:
            raise ValueError('No columns found in the dataframe, make sure to define mappings correctly')

        sum_col_daily = {col: 'sum' for col in cols_daily}
        # dynamic variables based on the zone or profile level aggregation
        if zone == True:
            attributes = ['proxy_date', 'zone']
            mape_column_order = 2
        elif zone == False:
            attributes = 'proxy_date'
            mape_column_order = 1

        df = df_hourly.copy()
        # Group by proxy_date and calculate the sum of the columns
        df = df.groupby(attributes).agg(sum_col_daily).reset_index()
        # Add columns for MAPE for both forecast and backcast
        for mapping in self.column_mappings:
            f, b, s, f_abs_e, b_abs_e, s_abs_e, f_m, b_m = mapping
            if all(col in df.columns for col in [s_abs_e, f_abs_e, b_abs_e]):
                df[f_m] = df[f_abs_e] / df[s_abs_e]
                df[b_m] = df[b_abs_e] / df[s_abs_e]

        # remove any Nan or inf  rows based on the mape columns
        df = df.replace([np.inf, -np.inf], np.nan)
        df = df.dropna()

        # Remove rows where both forecast and backcast columns contain 0.0
        forecast_cols = [col for col in cols_daily if 'forecast' in col]
        backcast_cols = [col for col in cols_daily if 'backcast' in col]
        # Remove rows where both forecast and backcast columns contain 0.0
        for f_col, b_col in zip(forecast_cols, backcast_cols):
            if f_col in df.columns and b_col in df.columns:
                df = df[~((df[f_col] == 0.0) & (df[b_col] == 0.0))]

        # Dynamic column names for the MAPE columns
        # Extract columns that contain 'mape'
        mape_columns = [col for col in df.columns if 'mape' in col]
        # Extract other columns
        other_columns = [col for col in df.columns if 'mape' not in col]
        # Define the desired order for 'mape' columns
        mape_insert_position = mape_column_order
        # Create the new column order
        new_columns_order = other_columns[:mape_insert_position] + mape_columns + other_columns[mape_insert_position:]
        # Reorder the DataFrame columns
        df = df[new_columns_order]

        # Zone pivot
        if zone == True:
            cols_pivot_order = [
                col for col in df.columns
                for mapping in self.column_mappings
                if col in mapping
            ]
            d_mape_zone_pivot = df.pivot(index='proxy_date', columns='zone', values=cols_pivot_order)
            # Flatten the MultiIndex columns
            d_mape_zone_pivot.columns = ['_'.join(map(str, col)).strip() for col in d_mape_zone_pivot.columns.values]
            # Reset the index if needed
            d_mape_zone_pivot.reset_index(inplace=True)
            self.logger.info(f'Daily ZONE MAPE aggregation completed for {len(df)} rows')
            return d_mape_zone_pivot
        elif zone == False:
            self.logger.info(f'Daily Portfolio MAPE aggregation completed for {len(df)} rows')
            return df


    def save_to_excelspreadsheet(self, client_name: str, **kwargs):
        self.logger.info(f'Saving Excel file for {client_name}...')
        filepath = f'./output/{client_name}_performance.xlsx'
        with pd.ExcelWriter(filepath, mode='w') as writer:
            for sheet_name, df in kwargs.items():
                df.to_excel(writer, sheet_name=sheet_name, index=False)
        self.logger.info(f'Excel file saved to: {filepath}')

        
if __name__ == '__main__':
    # Variables
    file1 = './athena_sql_results/client-settlement_mape_w_weather.csv'
    raw_jp = pd.read_csv(file1)
    raw_jp.columns = raw_jp.columns.str.lower()

    # Object instantiation 1
    calc = MapeCalculation(raw_jp)
    
    hourly_portfolio = calc.hourly_aggregation()
    mape_portfolio = calc.daily_mape_aggregation(hourly_portfolio)
    hourly_zone = calc.hourly_aggregation(zone=True)
    mape_zone = calc.daily_mape_aggregation(hourly_zone, zone=True)

    calc.save_to_excelspreadsheet(
    client_name='client_jp',
    raw_data=raw_jp,
    hourly_portfolio=hourly_portfolio,
    daily_portfolio_mape=mape_portfolio,
    hourly_zone=hourly_zone,
    daily_zone_mape=mape_zone
    # Add more DataFrames as needed
    )


    # Variables
    file2 = './athena_sql_results/client_ops.csv'
    raw_ops = pd.read_csv(file2)
    raw_ops.columns = raw_ops.columns.str.lower()
    
    # Object instantiation 2
    calc2 = MapeCalculation(raw_ops)
    hourly_portfolio = calc2.hourly_aggregation()
    mape_portfolio = calc2.daily_mape_aggregation(hourly_portfolio)
    hourly_zone = calc2.hourly_aggregation(zone=True)
    mape_zone = calc2.daily_mape_aggregation(hourly_zone, zone=True)

    calc2.save_to_excelspreadsheet(
    client_name='client_ops',
    raw_data=raw_ops,
    hourly_portfolio=hourly_portfolio,
    daily_portfolio_mape=mape_portfolio,
    hourly_zone=hourly_zone,
    daily_zone_mape=mape_zone
    # Add more DataFrames as needed
    )
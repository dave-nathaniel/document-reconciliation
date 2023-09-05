import pandas as pd

'''
	Methods defined in this file are used to transform DataFrames in some way.

'''


def transform_cols(df, **kwargs):
	action = kwargs.get("action")
	col = kwargs.get("col")

	if action:
		if col and action:
			df[col] = df.apply(action, axis=1)
		else:
			df.columns = map(action, df.columns)
			
	return df

def filter_entries(df, **kwargs):
	filter_on_col = kwargs.get("filter_on")
	include_only = kwargs.get("include_only")
	exclude_only = kwargs.get("exclude_only")

	if filter_on_col:
		if include_only:
			df = df[df[filter_on_col].isin(include_only)]

		if exclude_only:
			df = df[~df[filter_on_col].isin(include_only)]

	return df


def include_only_cols(df,  **kwargs):
	include_only = kwargs.get("include_only")

	if include_only:
		df = df.loc[:, include_only]

	return df


def create_calculated_column(df, **kwargs):
	col = kwargs.get("col_name")
	generator = kwargs.get("generator")
	
	if col and generator:
		df = transform_cols(df, col=col, action=generator, **kwargs)

	return df


def create_row(df, **kwargs):
	row = kwargs.get("row")

	if row:
		df = pd.concat([df, pd.DataFrame([row])], ignore_index=True)
	
	return df


def format_date_time(df, **kwargs):
	col = kwargs.get("col_name")
	cols = kwargs.get("col_names")
	dt_format = kwargs.get("df_format")

	if dt_format:
		if col:
			df[col] = pd.to_datetime(df[col])
			df[col] = df[col].dt.strftime(dt_format)

		if cols and isinstance(cols, list):
			for c in cols:
				df[c] = pd.to_datetime(df[c])
				df[c] = df[c].dt.strftime(dt_format)

	return df


def create_total_amount_row(df, **kwargs):

	try:
		report_name = kwargs.get("report_name", None) or "DEFAULT"

		df.loc[:, 'settlement_impact'].astype(float)
		last_date_time = df['local_date_time'].iloc[-1]

		total_row = {
			'datetime': df['datetime'].iloc[-1], 
			'local_date_time': last_date_time, 
			'remarks': f"SETTLEMENT FIGURE {report_name.upper()} REPORT {last_date_time}",
			'settlement_impact': df['settlement_impact'].sum(), 
			'message_type': 1, 
			'settlement_impact_desc': 'Special_Case'
		}

		df = create_row(df, row=total_row)

	except KeyError:
		#This error will most likely be thrown for an empty dataframe
		pass

	return df


def format_amount_2fp(df, **kwargs):
	col = kwargs.get("col", None)

	if col:
		df.loc[:, col].astype(float).round(2)

	return df
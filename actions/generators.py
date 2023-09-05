import pandas

'''
	Methods and lambda functions defined in this file are used for creating calculated rows

'''

zero = lambda x: 0

format_amt_str = lambda x: str(x)


def spb_remark_generator(row): 
	return f"RRN {row['retrieval_reference_nr']} AUTH {row['auth_id']} STAN {row['stan']} PAN {row['pan']} {row['terminal_id']} {row['settlement_impact']} {row['trxn_category']}"


def spb_tx_id_generator(row):
	return f"{row['retrieval_reference_nr']}{row['terminal_id']}"



#For LEDGER
def ledger_remark_generator(row):
	if pandas.notnull(row['at.unique.id']):
		#For entries from the terminal
		return f"RRN {row['rrn']} AUTH {row['auth.code']} STAN {row['stan']} PAN {row['pan.number']} {row['terminal.id']} {row['amt.lcy']}" 
	else:
		#For manual posting
		return f"{row['our.ref']} {row['narrative']}"


def ledger_stan_generator(row): 
	return str(row['at.unique.id'])[2:8] if pandas.notnull(row['at.unique.id']) else ''


def ledger_tx_id_generator(row):
	if pandas.notnull(row['rrn']):
		#For entries from the terminal
		return f"{row['rrn']}{row['terminal.id']}"
	elif pandas.notnull(row['our.ref']):
		#For manual posting
		return f"{row['our.ref']}"
	else:
		return ''


def generate_from_combination(column_names):
	return lambda row: ''.join([str(row[item]) for item in column_names if pandas.notnull(row[item])])
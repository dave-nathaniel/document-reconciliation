
import document_reconciliation.actions.mutations as mutate
import document_reconciliation.actions.generators as generator



ledger_sequence = [
	{
		"action": mutate.create_calculated_column,
		"params": {
			"col_name": "stan",
			"generator": generator.ledger_stan_generator
		}
	},
	{
		"action": mutate.create_calculated_column,
		"params": {
			"col_name": "remarks",
			"generator": generator.ledger_remark_generator
		}
	},
	{
		"action": mutate.create_calculated_column,
		"params": {
			"col_name": "transaction_id",
			"generator": generator.ledger_tx_id_generator
		}
	},
	{
		"action": mutate.create_calculated_column,
		"params": {
			"col_name": "secondary_transaction_id",
			"generator": generator.generate_from_combination(['rrn', 'amt.lcy'])
		}
	},
	{
		"action": mutate.create_calculated_column,
		"params": {
			"col_name": "age",
			"generator": generator.zero
		}
	},
	{
		"action": mutate.format_date_time,
		"params": {
			"col_names": ["booking.date", "val.date"],
			"dt_format": '%d-%b-%y'
		}
	},
]




spb_sequence = [
	{
		"action": mutate.create_calculated_column,
		"params": {
			"col_name": "remarks",
			"generator": generator.spb_remark_generator
		}
	},
	{
		"action": mutate.create_calculated_column,
		"params": {
			"col_name": "transaction_id",
			"generator": generator.spb_tx_id_generator
		}
	},
	{
		"action": mutate.create_calculated_column,
		"params": {
			"col_name": "secondary_transaction_id",
			"generator": generator.generate_from_combination(['retrieval_reference_nr', 'settlement_impact'])
		}
	},
	{
		"action": mutate.create_calculated_column,
		"params": {
			"col_name": "age",
			"generator": generator.zero
		}
	},
	{
		"action": mutate.format_date_time,
		"params": {
			"col_names": ["datetime", "local_date_time"],
			"dt_format": '%d-%b-%y'
		}
	},
]
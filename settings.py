'''
	Required Definitions (your code WILL break without these):
		* PROCESS_NAME
		* PROCESS_DESC
		* PROCESS_INPUTS
			- "files" (obj) 
			- "folders" (obj) 
		* PROCESS_OUTPUTS
		* GROUP_CONFIG

'''

'''
	Level 1:
		files:  An object which the supplied input will be the path to an excel file
			files[file_name]: (e.g. "ledger", "prev_unrec") an object containing definitions on the file named in by it's key.
				files[file_name]["info"]: A text description of the file.
				files[file_name]["config"]: An object containing definitions for different libraries should work with this file.
				files[file_name]["mutations"]: An object containing definition of functions that should be executed on this file.
					files[file_name]["mutations"]["read"]: An ordered array of functions to be executed on this file after it is read.
					files[file_name]["mutations"]["join"]: An ordered array of functions to be executed when this file is being joined with another file.
					files[file_name]["mutations"]["write"]: An ordered array of functions to be executed when this file is being written.
		


'''

import document_reconciliation.actions.mutations as mutate
import document_reconciliation.actions.generators as generator




PROCESS_NAME = "SterlingReconcile"


PROCESS_DESC = "Sterling Bank Reconciliation"


default_document_section = "__default__"


LEDGER_READ_COLS = {
	'booking.date': str,
	'val.date': str,
	'amt.lcy': float,
	'narrative': str,
	'at.unique.id': str,
	'our.ref': str,
	'rrn': str,
	'pan.number': str,
	'terminal.id': str,
	'auth.code': str,
}


SPB_READ_COLS = {
	'retrieval_reference_nr': str, 
	'stan': str,
	'pan': str,
	'terminal_id': str,
	'settlement_impact': float,
	'settlement_impact_desc': str,
	'trxn_category': str,
	'local_date_time': str,
	'datetime': str,
	'message_type': str,
	'auth_id': str,
}




PROCESS_INPUTS = {

	"files": {
		"ledger": {
			"info": "The ledger file.",
			"config": {

				"pandas": {
					"read": {
						"sep": '|',
						"chunksize": 10000,
						"header": 0,
						"usecols": LEDGER_READ_COLS,
						"skiprows": [1]
					},

					"write": {},
				},
			},

			"mutations": {
				"read": [
					{
						"action": mutate.transform_cols,
						"params": {
							"action": str.lower
						}
					},
                ],
				"join": [],
				"write": [],
			},

		},

		"prev_unrec": {
			"info": "Previous day unreconciled file.",
			"config": {

				"pandas": {
					"read": {},
					"write": {},
				},
			},
			"mutations": {
				"read": [],
				"join": [],
				"write": [],
			},

			"sections": {
	            "SETTLEMENT OUTSTANDING": {
	                "name": "SETTLEMENT OUTSTANDING",
	                "config": {
	                    "pandas": {
	                        "read": {},
	                        "write": {}
	                    },
	                },
	                "mutations": {
	                    "read": [],
	                    "join": [],
	                    "write": []
	                }
	            },
	            "LEDGER OUTSTANDING": {
	                "name": "LEDGER OUTSTANDING",
	                "config": {
	                    "pandas": {
	                        "read": {},
	                        "write": {}
	                    },
	                },
	                "mutations": {
	                    "read": [],
	                    "join": [],
	                    "write": []
	                }
	            }
	        },

		},
	},

	"folders": {
		"atm": {
			"info": "The SPB_ATM_TRANSFER directory.",
			"config": {
				"pandas": {
					"read": {},
					"write": {},
				},
			},

			"mutations": {
				"read": [],
				"join": [],
				"write": [],
			},
		},

		"autopay": {
			"info": "The SPB_AUTOPAY directory.",
			"config": {
				"pandas": {
					"read": {},
					"write": {},
				},
			},
			"mutations": {
				"read": [],
				"join": [],
				"write": [],
			},
		},

		"others": {
			"info": "The SPB_OTHER_TRANSFER directory.",
			"config": {
				"pandas": {
					"read": {},
					"write": {},
				},
			},
			"mutations": {
				"read": [],
				"join": [],
				"write": [],
			},
		},

		"qt": {
			"info": "The SPB_QT_TRANSFER directory.",
			"config": {
				"pandas": {
					"read": {},
					"write": {},
				},
			},
			"mutations": {
				"read": [],
				"join": [],
				"write": [],
			},
		},
	}
}


PROCESS_OUTPUTS = {
	"folders": {
		"main_output": {
			"info": "The output directory.",
			"config": {
				"create_if_not_exist": True,
			},
		},
	},
}






'''
	Grouped configurations to define settings that should apply to all members of the group.
	If there are conflicting defininitions, the one defined directly in object takes precedence
	over the group definintion.

'''
GROUPING = [
	{
		"group": [
			'PROCESS_INPUTS.others', 
			'PROCESS_INPUTS.atm', 
			'PROCESS_INPUTS.autopay', 
			'PROCESS_INPUTS.qt', 
			'PROCESS_INPUTS.prev_unrec', 
			'PROCESS_INPUTS.ledger', 
			'PROCESS_OUTPUTS.main_output',
		],

		"config": {
			"argparse": {
				"type": str, 
				"required": True
			},
		},
	},
		
	{
		"group":[
			'PROCESS_INPUTS.others', 
			'PROCESS_INPUTS.atm', 
			'PROCESS_INPUTS.autopay', 
			'PROCESS_INPUTS.qt'
		],

		"config": {
			"pandas": {
				"read":{
					"header": 0,
					"sep": ",",
					"chunksize": 10000,
					"usecols": SPB_READ_COLS
				},
				"write": {},
			},
		},
		"mutations": {
			"read": [
				{
					"action": mutate.transform_cols,
					"params": {
						"action": str.lower
					}
				},
				{
					"action": mutate.filter_entries,
					"params": {
						"filter_on": "settlement_impact_desc",
						"include_only": ['Amount_Payable', 'Amount_receivable', 'Special_Case'],
					}
				},
				# {
				# 	"action": mutate.include_only_cols,
				# 	"params": {
				# 		"include_only": ['datetime', 'local_date_time', 'settlement_impact', 'message_type', 'settlement_impact_desc', 'terminal_id', 'retrieval_reference_nr', 'stan', 'pan', 'trxn_category', 'auth_id'],

				# 	}
				# },
			],
			"join": [],
			"write": [],
		}
	},
]
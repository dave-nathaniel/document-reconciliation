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


PROCESS_NAME = "Reconciliation Process Name"
PROCESS_DESC = "Description of what this process does."

#A name to access the default sheet that was read when no sheets were specified.
default_document_section = "__default__"

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

PROCESS_INPUTS = {

	"files": {
		"name": {
			"info": "",
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
	            "Sheet1": {
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
	        },

		},
	},

	"folders": {
		"name": {
			"info": "",
			"config": {},
			"mutation": {
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
			"info": "",
			"create_if_not_exist": True,
		},

	},
}






'''
	"argparse_config":
		"options" contain the arguments that should be passed to the Argparse library for every argument in the "group".
	
	"pandas":
		"options" contain arguments that should be passed [to the reader method] when reading files in each "group".

'''
GROUP_CONFIG = {
	"argparse": [
		{
			"group": [],
			"options": {
				"type": str, 
				"required": True
			}
		}
	],

	"pandas": {
		"read":{
			"group": [],

			"options": {
				"header": 0,
				"dtype": str,
				"sep": ",",
			}
		},
		"write": {}
	},
}

# import logging
import pandas as pd
import pprint
import sys
from copy import deepcopy


from document_reconciliation.core.reconcile.utilities import ReconciliationUtilities
from document_reconciliation.core.documents.document import Document
import document_reconciliation.sequence as sequence

 
from document_reconciliation.actions import update_dict
from document_reconciliation.actions.mutations import create_total_amount_row, include_only_cols

# import line_profiler


from document_reconciliation import __RECON_SETTINGS_MODULE__ as config



class SterlingReconcile(ReconciliationUtilities):

	"""docstring for SterlingReconcile"""


	def __init__(self, config=config):
		super().__init__(config)
		super().make_args(make_with=['PROCESS_INPUTS', 'PROCESS_OUTPUTS'])


	
	def sequence(self, report):
		'''
			Get the sequence of steps defined in sequence.py which is representation of actions performed
			on the documents, in the order they are performed in, to achieve whatever the objective of this 
			process is.
		'''
		report = f"{report}_sequence"
		steps = getattr(sequence, report)

		if steps: 
			return steps

		return None




def main():
	'''
		Main method to handle data flow of the process
	'''


	#Create an instance of the primary class and call the methods to treat creating and parsing arguments.
	recon_util = SterlingReconcile()

	#Dictionary of Documents required for the process
	process_docs = dict()

	#Main output directory
	output_dir = getattr(recon_util.settings, "PROCESS_OUTPUTS")["folders"]["main_output"]["path"]

	'''
		FOR INPUT FILES:
		Read the files, make each of them a Document object.
	'''
	process_docs.update(
		dict(
			[( lambda i,j: (i, Document({i:j})) )(name, file) for name, file in recon_util.__inputs__("files").items()]
		)
	)

	'''
		FOR INPUT FOLDERS:
		Open each of the folders, read their files, make each file a Document object, combine all into 
		one Document object.
	'''
	for name, folder in recon_util.__inputs__("folders").items():
		folder_files = folder.pop("files")

		#A list of  the Document objects representing each file in the folder.
		docs = []

		#Make each created object into a Document object
		for file in folder_files:
			file_dict = deepcopy(folder)
			file_dict["path"] = file
			doc = Document(dict({name:file_dict}))
			doc._set_dataframe(create_total_amount_row(doc.dataframe, report_name=name))
			docs.append(doc)

		#Combine all the Document objects to the first created Document object to make them one Document object, representing one file.
		docs = docs[0]._append_document(docs[1:])

		#Create the key "SPB" in process_docs that points to the combined Document object, or combine with others if the key already exists.
		try:
			process_docs["spb"] = process_docs["spb"]._append_document([docs])
		except KeyError:
			process_docs.update({"spb": docs})





	'''
		#	MAIN TRANSFORMATION
		#	
		#	[*] Mutate the combined dataframe by:
		#		Setting the sequencial steps as a mutation for the Document object.
		#		Applying the mutation.
		#	[*] Drop entries with blank values for tx_id so that they don't falsify our reconciliation output.
		#	[*] Handle Duplicates
	#
	'''
	#############################################################
	#
	#					
	#					INTERSWITCH (SPB, TSS, SETTLEMENT)
	#
	#
	#############################################################
	##

	spb = process_docs["spb"]

	#Exclude the 'Special Case' entries from the dataframe, i.e. the "total amount" row we created earlier
	spb_special = spb.dataframe[spb.dataframe['settlement_impact_desc'] == 'Special_Case']
	#Drop these entries from the dataframe of the combined Document object.
	spb.dataframe.drop(spb_special.index, inplace=True)

	#[INTERSWITCH]----------------MUTATE------------------##
	#
	spb._set_mutations(recon_util.sequence('spb'))
	spb._apply_mutations()

	spb_df = spb.dataframe

	##[INTERSWITCH]----------------COLUMN CLEANUP------------------##
	##
	spb_df.loc[:, 'transaction_id'].dropna()
	spb_df.loc[:, 'secondary_transaction_id'].dropna()

	##[INTERSWITCH]----------------RENAME COLUMNS------------------##
	##
	new_spb_columns = {
		"datetime": "Postdate", 
		"local_date_time": "Valdate", 
		"remarks": "Details", 
		"settlement_impact": "Amount",
		"retrieval_reference_nr": "rrn", 
		"age": "Age", 
		"transaction_id": "transaction_id",
		"secondary_transaction_id": "secondary_transaction_id",
	}

	spb_df = include_only_cols(spb_df, include_only=new_spb_columns.keys())
	spb_df.rename(columns=new_spb_columns, inplace=True)

	#############################################################
	#
	#					
	#					LEDGER
	#
	#
	#############################################################
	##

	ledger = process_docs["ledger"]

	##[LEDGER]----------------MUTATE------------------##
	##
	ledger._set_mutations(recon_util.sequence('ledger'))
	ledger._apply_mutations()

	ledger_df = ledger.dataframe

	#Isolate the manual postings
	ledger_manual_posting = ledger_df[ledger_df['at.unique.id'].isna()]
	#Drop these entries from the dataframe of the combined Document object.
	ledger_df.drop(ledger_manual_posting.index, inplace=True)

	##[LEDGER]----------------COLUMN CLEANUP------------------##
	##
	ledger_df.loc[:, 'transaction_id'].dropna()
	ledger_df.loc[:, 'secondary_transaction_id'].dropna()

	##[LEDGER]----------------RENAME COLUMN------------------##
	##
	new_ledger_columns = {
		"booking.date": "Postdate", 
		"val.date": "Valdate", 
		"remarks": "Details", 
		"amt.lcy": "Amount", 
		"rrn": "rrn", 
		"age": "Age", 
		"transaction_id": "transaction_id",
		"secondary_transaction_id": "secondary_transaction_id"
	}

	ledger_df = include_only_cols(ledger_df, include_only=new_ledger_columns.keys())
	ledger_df.rename(columns=new_ledger_columns, inplace=True)
	
	



	'''
		#	MAIN RECONCILIATION
		#
		#	[*] Handle Duplicates
		#	[*] 
		#	[*] 
	'''

	id_col = "transaction_id"
	secondary_id_col = "secondary_transaction_id"

	##
	##----------------HANDLE DUPLICATES------------------##
	##
	##[INTERSWITCH]
	##
	spb_df_dups = recon_util.drop_and_get_duplicates(spb_df, [id_col])
	#Identify duplicates that have inverse copies (i.e. with a positive and negative entry)
	spb_inverse_dups = spb_df_dups.groupby(id_col).filter(
		lambda x: (x['Amount'].sum() == 0) and len(x) == 2
	)

	spb_df_dups.drop(spb_inverse_dups.index, inplace=True)

	##
	##[LEDGER]
	##
	ledger_df_dups = recon_util.drop_and_get_duplicates(ledger_df, [id_col])
	# Identify duplicates that have inverse copies (i.e. with a positive and negative entry)
	ledger_inverse_dups = ledger_df_dups.groupby(id_col).filter(
		lambda x: (x['Amount'].sum() == 0) and len(x) == 2
	)

	ledger_df_dups.drop(ledger_inverse_dups.index, inplace=True)


	#############################################################
	#
	#					
	#					INTERSWITCH (SPB, TSS) RECONCILIATION
	#
	#
	###############################################################
	###
	###[INTERSWITCH]----------------RECONCILIATION------------------##
	###
	spb_reconciled = spb_df[
		spb_df[id_col].isin(ledger_df[id_col])
	]

	spb_unreconciled = spb_df[
		~spb_df[id_col].isin(ledger_df[id_col]) 
	]



	#############################################################
	#
	#					
	#					LEDGER RECONCILIATION
	#
	#
	###############################################################
	##
	##[LEDGER]----------------RECONCILIATION------------------##
	##
	ledger_reconciled = ledger_df[
		ledger_df[id_col].isin(spb_df[id_col])
	]

	ledger_unreconciled = ledger_df[
		~ledger_df[id_col].isin(spb_df[id_col])
	]


	##[LEDGER]----------------SORTING------------------##
	##
	spb_reconciled.sort_values(by=['Amount'], ascending=True, inplace=True)
	spb_unreconciled.sort_values(by=['Amount'], ascending=True, inplace=True)

	ledger_reconciled.sort_values(by=['Amount'], ascending=True, inplace=True)
	ledger_unreconciled.sort_values(by=['Amount'], ascending=True, inplace=True)




	print(f"{output_dir}SETTLEMENT BULK.csv")
	spb_special.to_csv(f"{output_dir}SETTLEMENT BULK.csv", index=False)

	print(f"{output_dir}LEDGER MANUAL.csv")
	ledger_manual_posting.to_csv(f"{output_dir}LEDGER MANUAL.csv", index=False)


	#############################################################
	#
	#					
	#						WRITING OUTPUT
	#
	#
	###############################################################
	##
	##[TSS]----------------OUTPUT------------------##
	##
	print(f"{output_dir}SETTLEMENT OUTSTANDING.csv")
	spb_unreconciled.to_csv(f"{output_dir}SETTLEMENT OUTSTANDING.csv", index=False)

	print(f"{output_dir}SETTLEMENT RECONCILED.csv")
	spb_reconciled.to_csv(f"{output_dir}SETTLEMENT RECONCILED.csv", index=False)


	##[LEDGER]----------------OUTPUT------------------##
	##

	print(f"{output_dir}LEDGER RECONCILED.csv")
	ledger_reconciled.to_csv(f"{output_dir}LEDGER RECONCILED.csv", index=False)

	print(f"{output_dir}LEDGER OUTSTANDING.csv")
	ledger_unreconciled.to_csv(f"{output_dir}LEDGER OUTSTANDING.csv", index=False)







if __name__ == '__main__':

	print("\n"*2)

	# profile = line_profiler.LineProfiler(main)
	# profile.enable()
	main()
	# profile.disable()
	# profile.print_stats()
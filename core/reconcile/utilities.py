import os, sys
import pandas as pd
from copy import deepcopy
import logging
import argparse
import pprint
from document_reconciliation.core.exceptions import InvalidGroupingException
from document_reconciliation.actions import merge_dictionaries

from document_reconciliation import __RECON_SETTINGS_MODULE__ as __SETTINGS_MODULE__


'''
	Custom actions for 
'''


class ReconciliationUtilities(object):

	'''
		This class defines methods for logic frequently employed
		in the process of reconciliation.
	'''

	def __init__(self, config=__SETTINGS_MODULE__):

		self._settings = config

		self._process_name = None
		self._process_desc = None

		self._process_name = self._settings.PROCESS_NAME
		self._process_desc = self._settings.PROCESS_DESC

		self._initialize()



	@property
	def settings(self):
		return self._settings


	@property
	def process_name(self):
		return self._process_name


	@property
	def process_desc(self):
		return self._process_desc


	@property
	def input_args(self):
		return self._input_args


	@process_name.setter
	def set_process_name(self):
		self._process_name = self._settings.PROCESS_NAME


	@process_desc.setter
	def set_process_desc(self):
		self._process_desc = self._settings.PROCESS_DESC


	
	def __inputs__(self, key):
		inputs = getattr(self.settings, "PROCESS_INPUTS")
		return inputs[key]

	
	def __outputs__(self, key):
		outputs = getattr(self.settings, "PROCESS_OUTPUTS")




	'''
		Begin Main Methods
	'''

	def _initialize(self, ):
		'''
			Perfoam initialization tasks like modifying definitaions in the config
		'''
		self._update_with_group()


	def _update_with_group(self,):
		'''
			Update definition of files and folders defined in "PROCESS_INPUTS" and "PROCESS_OUTPUTS"
			to include configurations directly in the object for any group that the object was included in.

			This method only applied the group configuration to objects in the "PROCESS_INPUTS" and 
			"PROCESS_OUTPUTS" but can be refactored made more versatile.
		'''

		settings = self.settings

		groups = getattr(settings, "GROUPING")

		for group in groups: #Each group
			members = group.get("group", []) #The members of the group 
			merge_attributes = [attr for attr in group if attr != 'group'] #The attributes that should be merged to the members of the group
			for member in members:
				try:
					member_key_category, member_key_name = member.split(".")
					member_category = getattr(settings, member_key_category)
				
					for member_parent_key in member_category.keys():
						member_main_dict = member_category[member_parent_key].get(member_key_name, None)
						if member_main_dict:
							for attribute in merge_attributes:
								attribute_from_group = dict({attribute:group.get(attribute, {})})
								merged = merge_dictionaries(member_main_dict, attribute_from_group)
								member_main_dict.update(merged)

				except AttributeError as e:
					raise InvalidGroupingException(e)

		self._settings = settings


	def make_args(self, make_with, do_mapping=True):

		"""
			Create and parse command-line arguments using argparse.

			Args:
				make_with (list): A list of objects with which we should represent it's children as arguements an receive inputs.

			Returns:
				argparse.Namespace: An object containing the parsed command-line arguments.

			Raises:
				None.

		"""

		parser = argparse.ArgumentParser(description=self.process_desc)

		objects_to_argument = {}

		#Get and combine all objects in each category defined in the "make_with" object (e.g. files, folders) into one object "objects_to_argument".
		for item in make_with:
			item_contents = getattr(self.settings, item)
			for category in list(item_contents.keys()):
				'''
					Modify the keys of content items (which are our main target items) such that the the key containes the catergory (e.g. "ledger" which is categorized under "files", becomes "ledger_file").
					We make the category[0:] name singluar by remove the "s". 

					We're doing this so that the user knows which input is supposed to be a directory and which is to be a filepath.
				'''

				singular_category = category.rstrip("s") if category.endswith("s") else category

				target_obj = item_contents[category]

				target_obj =  {renamed_key: value for (renamed_key, value) in map(lambda cat: (f"{cat}.{singular_category}", target_obj[cat]), target_obj.keys())}
				
				objects_to_argument.update(target_obj)


		'''
			Start adding the arguments
		'''
		for key in objects_to_argument.keys():
			#Modify the dest name to the original key before we added the ".category"
			dest_name = key.split(".")[0]

			# Modify the key to create the argument name
			arg_name = "--" + key.replace("_", "-").replace(".", "-")

			#The kwargs dict to be passed to the created arg.
			key_argparse_config = {}

			try:
				# Get the info for the help message
				key_argparse_config["help"] = objects_to_argument[key]["info"]
			except KeyError:
				info = ""

			try:
				# Get the argparse_config for the current key
				config = objects_to_argument[key]["config"]["argparse"]
				# Add argparse_config (if any was defined) as kwargs
				key_argparse_config.update(config)

			except KeyError:
				pass

			
			# Add the argument with the help message
			parser.add_argument(arg_name, dest=dest_name, **key_argparse_config)


		self._input_args = parser.parse_args()

		if do_mapping:
			self.map_args(make_with)

		return self


	def map_args(self, maps_to=None):
		'''
			Maps the files and folders defined in the config to their actual path gotten from the args.
		'''

		settings = self.settings

		for definition in maps_to:
			item = getattr(settings, definition)
			for key, value in item.items():
				if isinstance(value, dict):
					for subkey in value:
						if hasattr(self.input_args, subkey):
							path = getattr(self.input_args, subkey)
							if os.path.isdir(path):
								value[subkey]["files"] = [os.path.join(path, file) for file in os.listdir(path)]
							elif os.path.isfile(path):
								pass
							else:
								if "create_if_not_exist" in value[subkey].get("config", []).keys():
									os.makedirs(path) if value[subkey].get("config", [])["create_if_not_exist"] else None
								else:
									raise FileNotFoundError(f"Could not find the path defined for {subkey}: {path}")

							value[subkey]["path"] = path
			setattr(self._settings, definition, item)


	def remove_duplicates(df, column):
		"""Remove duplicates based on the specified column."""
		return df.drop_duplicates(subset=[column], keep=False)


	def drop_and_get_duplicates(self, df, subset_cols):
		# Sort the DataFrame by the subset columns
		df.sort_values(by=subset_cols, inplace=True)

		# Create a boolean mask to identify duplicates
		mask = df.duplicated(subset=subset_cols, keep=False)

		# Extract the duplicates into a new DataFrame
		duplicates_df = df[mask].copy()

		# Drop the duplicates from the original DataFrame
		df.drop_duplicates(subset=subset_cols, keep=False, inplace=True)

		return duplicates_df




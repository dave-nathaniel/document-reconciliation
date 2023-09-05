import os, sys
from pprint import pprint
import pandas as pd
from document_reconciliation import __RECON_SETTINGS_MODULE__
import multiprocess as mp
from copy import deepcopy
from functools import partial



__SETTINGS_MODULE__ =  __RECON_SETTINGS_MODULE__

__default_section__ = getattr(__SETTINGS_MODULE__, "default_document_section")


num_cores = 4  # Get the number of CPU cores


class Document:

    def __init__(self, doc_dict):
        """
        Initializes the Document object with the provided doc_dict.

        Args:
            doc_dict (dict): A dictionary containing the document configuration.

        """

        self._name = None
        self._config = None
        self._path = None
        self._mutations = None
        self._sections = {}
        self._has_sections = False
        self._dataframe = pd.DataFrame()

        self._initialize(doc_dict)

        


    @property
    def name(self):
        return self._name


    @property
    def path(self):
        return self._path


    @property
    def config(self):
        return self._config


    @property
    def mutations(self):
        return self._mutations


    @property
    def sections(self):
        return self._sections


    @property
    def has_sections(self):
        return self._has_sections


    @property
    def dataframe(self):
        return self._dataframe


    '''
        Setters
    '''

    def _set_name(self, name):
        """
        Sets the name of the Document based on the first key in doc_dict.

        """
        self._name = name


    def _set_path(self, path):
        """
        Sets the path of the Document based on the 'path' value in doc_dict.

        """
        self._path = path


    def _set_config(self, config):
        """
        Sets the config of the Document based on the 'config' value in doc_dict.

        """
        self._config = config


    def _set_has_sections(self, sections):

        if sections:
            self._sections = sections
            self._has_sections = True
        else:
            self._has_sections = False


    def _set_dataframe(self, df):
        self._dataframe = df


    def _set_mutations(self, value):
        """
        Sets the mutations of the Document based on the 'mutations' value in doc_dict.

        """
        self._mutations = value


    def _set_sections(self):
        """
        Sets the sections of the Document based on the 'sections' value in doc_dict.

        If no sections are specified, a default section with the name '__default__' is created using the main configuration.

        """
        if self.has_sections:
            for section_name in self.sections:

                section = self.sections.get(section_name)

                section["path"] = self.dataframe

                self._sections[section_name] = DocumentSection(dict({section_name: section}))
        else:
            # No specific sections defined, use default section with main configuration
            self._sections[__default_section__] = self



    '''

        Methods
    '''


    def _initialize(self, doc_dict):
        """
            Initializes the Document object by setting the name, path, and sections.

        """
        #This must be the only key in doc_dict so we can hard-code index 0
        document_name = list(doc_dict.keys())[0]

        doc_dict = doc_dict.get(document_name)

        self._set_name(document_name)
        self._set_config(doc_dict.get("config"))
        self._set_path(doc_dict.get("path"))

        self._set_has_sections(doc_dict.get("sections", False))
        
        self._set_mutations(doc_dict.get("mutations", {}).get("read", {}))

        self._read_file()

        self._set_sections()


    def _set_pandas_options(self, ):
        '''
            Fix the pandas read options to work right.

            1. Modify usecols option and also set the dtype.
        '''

        pandas_config = deepcopy(self.config.get("pandas", {}))

        #1
        if pandas_config["read"].get("usecols", False):
            # Get the delimiter for reading the file, defaulting to comma (",")
            delimiter = pandas_config["read"].get("sep", ",")
            # Get the columns to use from the configuration
            use_columns = pandas_config["read"]["usecols"]
            # Convert the column names to lowercase for consistency
            columns = [x.lower() for x in use_columns.keys()]
            # Read the first row of the CSV or Excel file to get column names and types
            # Use pd.read_csv if the file is a CSV, else use pd.read_excel
            columns_df = pd.read_csv(self.path, nrows=0, sep=delimiter) if self.path.endswith('.csv') else pd.read_excel(self.path, nrows=0, sep=delimiter)
            # Filter the found columns based on the columns present in the DataFrame
            # and match them against the lowercase version of the specified columns
            found_columns = [x for x in filter(lambda x: x.lower() in columns, columns_df.columns.tolist())]
            # Add the dtype pandas option and initialize it to an empty dictionary to specify column types
            pandas_config["read"]["dtype"] = {}
            # Update the dtype dictionary with the specified types for the found columns
            # The use_columns dictionary maps column names to their specified types
            # Only include the columns that were found in the file
            for x in found_columns:
                pandas_config["read"]["dtype"][x] = use_columns[x.lower()]
            # Update the configuration with the found column names
            # Assign the lowercase column names to the "usecols" keys, this ensures that subsequent processing uses the consistent lowercase column names
            pandas_config["read"]["usecols"] = found_columns

        return pandas_config


    def _read_file(self):
        """
            Reads the file specified in the path using pandas read_csv or read_excel based on the file extension.

        """
        
        pandas_config = self._set_pandas_options()

        if self.has_sections:

            dataframe = pd.ExcelFile(self.path, **pandas_config["read"])
            self._set_dataframe(dataframe)

        else:
            
            try:
                if self.path.endswith('.csv'):
                    # If chunksize is defined, read in chunks
                    if 'chunksize' in pandas_config["read"].keys():
                        for df in pd.read_csv(self.path, **pandas_config["read"]):
                            self._set_dataframe(pd.concat([self.dataframe, df]))
                    else:
                        self._set_dataframe(pd.read_csv(self.path, **pandas_config["read"]))
                else:
                    self._set_dataframe(pd.read_excel(self.path, **pandas_config["read"]))

                self._apply_mutations()

            except FileNotFoundError as e:
                print(f"File not found: {self.path}")


    def _append_document(self, df_iter, **kwargs):
        """
        Append a DataFrame to the DataFrame of this document.

        Args:
            df_iter (Iterable): An iterable containing Document instances or DataFrame objects.
            **kwargs: Additional keyword arguments for customization.

        Keyword Args:
            on_before_join: Mutation to be applied to the incoming DataFrame before joining.
            on_join: Mutation to be applied to the resulting DataFrame each time a new DataFrame is added.
            on_complete_join: Mutation to be applied to the complete joined DataFrame.
            ... (any other valid DataFrame.concat method)

        """


        pool = mp.Pool(num_cores)

        on_before_join = kwargs.get("on_before_join", None)
        on_join = kwargs.get("on_join", None)
        on_complete_join = kwargs.get("on_complete_join", None)

        for item in df_iter:

            name = item.name or "default"

            if isinstance(item, Document):
                df = item.dataframe
            elif isinstance(item, pd.DataFrame):
                df = item.copy()
            else:
                raise ValueError("Invalid item type. Must be a Document instance or DataFrame.")


            df_len = len(df)

            if df_len > 0:
                chunk_size = df_len // num_cores  # Determine the chunk size
                chunks = [df[i:i + chunk_size] for i in range(0, chunk_size)]

                if on_before_join:
                    # Apply the helper function to each chunk using map()
                    result = pool.map(on_before_join, chunks)
                    df = pd.concat(result)

                self._set_dataframe(pd.concat([self.dataframe, df], ignore_index=True))

                if on_join:
                    result = pool.map(on_join, chunks)
                    self._set_dataframe(pd.concat(result))

        if on_complete_join:
            df_len = len(df)
            if df_len > 0:
                chunk_size = df_len // num_cores  # Determine the chunk size
                chunks = [df[i:i + chunk_size] for i in range(0, chunk_size)]
                result = pool.map(on_complete_join, chunks)

                self._set_dataframe(pd.concat(result))

        return self


    def _apply_mutations(self, ):
        """
            Applies mutations to the dataframe based on the specified mutation type.
            Always adds the name of the document being worked on to params kwargs.

        """
        # Dictionary Document Name
        doc_name = {"report_name": self.name}

        df_len = len(self.dataframe)
        
        if df_len > 0:
            chunk_size = df_len // num_cores  # Determine the chunk size

            pool = mp.Pool(num_cores)

            for mutation in self.mutations:

                chunks = [self.dataframe[i:i+chunk_size].copy() for i in range(0,self.dataframe.shape[0],chunk_size)]

                if isinstance(mutation, dict):
                    try:
                        mutation["params"].update(doc_name)
                    except KeyError:
                        mutation["params"] = doc_name

                    action = partial(mutation["action"], **mutation["params"])

                    df = pool.map(action, chunks)
                    self._set_dataframe(pd.concat(df))

                else:
                    #If the mutation is a lambda method, we can't pass kwargs.
                    action = partial(mutation, **doc_name) if not mutation.__name__ == '<lambda>' else mutation
                    df = pool.map(action, chunks)

                    self._set_dataframe(pd.concat(df))




class DocumentSection(Document):

    def __init__(self, doc_dict):
        self._doc_dict = doc_dict
        self._name = None
        self._config = None
        self._set_dataframe(pd.DataFrame())

        self._initialize()


    def _initialize(self, ):
        document_name = list(self._doc_dict.keys())[0]
        doc_dict = self._doc_dict.get(document_name)

        self._set_name(document_name)
        self._set_config(doc_dict.get("config"))
        self._set_path(doc_dict.get("path"))
        self._set_mutations(doc_dict.get("mutations", {}).get("read", {}))

        self._read_file()


    def _read_file(self):
        dataframe = pd.read_excel(self.path, sheet_name = self.name, **self.config["pandas"]["read"])
        self._set_dataframe(dataframe)
        self._apply_mutations()

        



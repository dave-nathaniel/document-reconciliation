import hashlib


class Aliases:
    def __init__(self):
        self.alias_dict = {}

    def set_aliases(self, alias_dict):
        # Update global dictionary with the received dictionary
        for key, value in alias_dict.items():
            if isinstance(value, str):
                self.alias_dict[key] = [value]
            elif isinstance(value, list):
                if len(value) != len(set(value)):
                    raise ValueError("Duplicate values found in alias list.")
                self.alias_dict[key] = value


    def resolve(self, string):
        # Search for the string in the dictionary values
        for key, value in self.alias_dict.items():
            if string in value:
                return key
            if isinstance(value, str) and string == value:
                return key

        print("String not found in aliases.")
        return string


    def get_aliases(self, input_list):
        modified_list = input_list.copy()
        aliases_dict = {}

        for i in range(len(modified_list)):
            resolved = self.resolve(modified_list[i])
            if resolved != modified_list[i]:
                modified_list[i] = resolved
                aliases_dict[input_list[i]] = resolved

        if aliases_dict:
            md5_hash = hashlib.md5(str(input_list).encode()).hexdigest()
            setattr(self, md5_hash, aliases_dict)
            return md5_hash, aliases_dict
        else:
            return None, aliases_dict


    def get_original(self, hash_value, order_list=None):
        if order_list is None:
            return getattr(self, hash_value, {})
        else:
            result_dict = {}
            aliases_dict = getattr(self, hash_value, {})
            for item in order_list:
                result_dict[item] = aliases_dict.get(item, item)
            return result_dict

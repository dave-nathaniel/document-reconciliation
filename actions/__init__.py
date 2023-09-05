'''
	Random useful methods that don't necessarily fit into a specific classification.
'''


def update_dict(d, k, v):
	d[k] = v
	return d


def merge_dictionaries(dict1, dict2):
	result = dict1.copy()

	for key, value in dict2.items():
		if key in result and isinstance(result[key], dict) and isinstance(value, dict):
			result[key] = merge_dictionaries(result[key], value)
		elif key in result and isinstance(result[key], list) and isinstance(value, list):
			result[key] = result[key] + value
		else:
			if key not in result:
				result[key] = value
			elif isinstance(result[key], dict) and isinstance(value, dict):
				result[key] = merge_dictionaries(result[key], value)
			elif isinstance(result[key], list) and isinstance(value, list):
				result[key] = result[key] + value
			else:
				result[key] = value

	return result
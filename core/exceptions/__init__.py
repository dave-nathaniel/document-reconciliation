from document_reconciliation import __RECON_SETTINGS_MODULE__


__SETTINGS_MODULE__ =  __RECON_SETTINGS_MODULE__


class InvalidGroupingException(Exception):
    def __init__(self, message):
        self.message = f"An invalid group definition was encountered in your GROUPING configuration: {message}"
        super().__init__(self.message)
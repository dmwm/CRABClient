from optparse import OptionParser


class SubCommand(object):

    ## setting visible = False doesn't allow the sub-command to be called from CLI
    visible = True
    usage = "usage: %prog [command-options] [args]" 

    def __init__(self, logger):
        """
        Initialize common client parameters
        """
        self.logger = logger

        self.parser = OptionParser(usage = self.usage, add_help_option = False)
        self.setOptions()

 
    def __call__(self, options):
        logging.info("This is a null command")
        raise NotImplementedException

    def setOptions(self):
        pass

    def printHelp(self):
        """
        Encapsulate the parsers print_help() method here in case there's some
        reason to override it.

        parser.print_help() actually prints to the screen, so just run that.
        """
        self.parser.print_help()


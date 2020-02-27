import logging


# Class that can be inherited to provide a .l logger method.
class SimpleLoggableBase(object):
    @property
    def l(self) -> logging.Logger:

        # Get RootLogger
        rootlogger = logging.getLogger()

        # Try to get this class' logger
        loggerobj = getattr(self, "__logger__", None)

        if not loggerobj:
            # Create this class logger if it doesn't exist.
            self.__logger__ = rootlogger.getChild(self.__class__.__name__)

        # return the logger
        return self.__logger__

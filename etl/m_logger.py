import logging


def get_logger(mod_name):
    logger = logging.getLogger(mod_name)
    if len(logging.getLogger().handlers) > 0:
        """ The Lambda environment pre-configures a handler logging to stderr.
            If a handler is already configured, basicConfig` does not execute.
            Thus we set the level directly. """
        logging.getLogger().setLevel(logging.INFO)
    else:
        logging.basicConfig(level=logging.INFO)
    return logger
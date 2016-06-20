def init_log(log_name, root = None):
    '''creates handlers for loging to file and console
        log_name: name of the log create.  this name can be used to retrieve the singleton log
        root: if passed, will create the log at this location, othwerwise will create log at temp folder location
    '''
    logger = logging.getLogger(log_name)
    if len(logger.handlers) > 0:
        return logger

    formatter = logging.Formatter(fmt='%(asctime)s - %(process)d - %(levelname)s - %(module)s.%(funcName)s - %(message)s')
    user = getpass.getuser().replace(' ', '_')
    dir = root
    if dir is None:
        dir = tempfile.gettempdir() + '/'

    if not os.path.exists(dir):
        os.makedirs(dir)
    dt_str =time.strftime('%m_%d_%Y')

    file_path =  os.path.join(dir, dt_str + '_' + user + '_' + log_name)
    f_handler = logging.FileHandler(file_path)
    f_handler.setLevel(logging.DEBUG)
    f_handler.setFormatter(formatter)

    c_handler = logging.StreamHandler()
    c_handler.setLevel(logging.INFO)
    c_handler.setFormatter(formatter)

    logger.addHandler(f_handler)
    logger.addHandler(c_handler)
    logger.debug('log initialized for ' + log_name)

def set_log_level(lvl, log_name = None):
    '''
    Sets log level for all handlers associated the named logger.
    Args:
        lvl = level to set (get this by calling logger.get_level()
        log_name: if None, then lvl will be set for all registered loggers.  
            Otherwise if a name is specified, it will only set the lvl for that logger
    '''
    log_keys = [log_key  for log_key in logging.Logger.manager.loggerDict.keys() if (log_key == log_name or log_name is None)]
    for key in log_keys:
        logger = logging.getLogger(key)
        if log_name is None or logger.name == log_name:
            logger.setLevel(lvl)
            for h in list(logger.handlers):
                h.setLevel(lvl)

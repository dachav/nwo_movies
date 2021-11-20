import logging
import logging.config


def configure_logger(name, log_path):
    logging.config.dictConfig({
        'version': 1,
        'formatters': {
            'default': {'format': '%(asctime)s - %(name)s : %(levelname)s - %(message)s', 'datefmt': '%Y-%m-%d %H:%M:%S'}
        },
        'handlers': {
            'console': {
                'level': 'WARNING',
                'class': 'logging.StreamHandler',
                'formatter': 'default',
                'stream': 'ext://sys.stdout'
            },
            'file': {
                'level': 'DEBUG',
                'class': 'logging.handlers.RotatingFileHandler',
                'formatter': 'default',
                'filename': log_path,
                'maxBytes': 10000,
                'backupCount': 5
            }
        },
        'loggers': {
            '': {  # root logger
                'handlers': ['console', 'file'],
                'level': 'INFO',
                'propagate': False
            },
            'load': {
                'handlers': ['console', 'file'],
                'level': 'INFO',
                'propagate': False
            },
            'extract': {
                'handlers': ['console', 'file'],
                'level': 'INFO',
                'propagate': False
            },
            'transform': {
                'handlers': ['console', 'file'],
                'level': 'INFO',
                'propagate': False
            },
            'util': {
                'handlers': ['console', 'file'],
                'level': 'INFO',
                'propagate': False
            },
            '__main__': {  # if __name__ == '__main__'
                'handlers': ['console', 'file'],
                'level': 'DEBUG',
                'propagate': False
            },
        },
        'disable_existing_loggers': True
    })
    return logging.getLogger(name)

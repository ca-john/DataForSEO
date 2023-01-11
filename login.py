
"""
This module is used to facilitate loggin into the DataForSEO API, it is used
by various other classes and modules.

Example:
    Examples can be given using either the ``Example`` or ``Examples``
    sections. Sections support any reStructuredText formatting, including
    literal blocks::

        $ python example_google.py

Section breaks are created by resuming unindented text. Section breaks
are also implicitly created anytime a new section starts.

Attributes:
    DEFAULT_EMAIL (str): The default email used as the id for the login
    DEFAULT_PWD (str): The default password/API token to use when loggin in

Todo:
    * For module TODOs
    * You have to also use ``sphinx.ext.todo`` extension

"""
from client import RestClient



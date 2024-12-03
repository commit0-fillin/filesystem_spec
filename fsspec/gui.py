import ast
import contextlib
import logging
import os
import re
from typing import ClassVar, Sequence
import panel as pn
from .core import OpenFile, get_filesystem_class, split_protocol
from .registry import known_implementations
pn.extension()
logger = logging.getLogger('fsspec.gui')

class SigSlot:
    """Signal-slot mixin, for Panel event passing

    Include this class in a widget manager's superclasses to be able to
    register events and callbacks on Panel widgets managed by that class.

    The method ``_register`` should be called as widgets are added, and external
    code should call ``connect`` to associate callbacks.

    By default, all signals emit a DEBUG logging statement.
    """
    signals: ClassVar[Sequence[str]] = []
    slots: ClassVar[Sequence[str]] = []

    def __init__(self):
        self._ignoring_events = False
        self._sigs = {}
        self._map = {}
        self._setup()

    def _setup(self):
        """Create GUI elements and register signals"""
        self.widget = pn.widgets.Select(name="Select an option")
        self._register(self.widget, "selection", "value")

    def _register(self, widget, name, thing='value', log_level=logging.DEBUG, auto=False):
        """Watch the given attribute of a widget and assign it a named event

        This is normally called at the time a widget is instantiated, in the
        class which owns it.

        Parameters
        ----------
        widget : pn.layout.Panel or None
            Widget to watch. If None, an anonymous signal not associated with
            any widget.
        name : str
            Name of this event
        thing : str
            Attribute of the given widget to watch
        log_level : int
            When the signal is triggered, a logging event of the given level
            will be fired in the dfviz logger.
        auto : bool
            If True, automatically connects with a method in this class of the
            same name.
        """
        if widget is not None:
            widget.param.watch(lambda event: self._signal(event), thing)
        self._sigs[name] = log_level
        if auto:
            self.connect(name, getattr(self, name))

    def _repr_mimebundle_(self, *args, **kwargs):
        """Display in a notebook or a server"""
        return self.widget._repr_mimebundle_(*args, **kwargs)

    def connect(self, signal, slot):
        """Associate call back with given event

        The callback must be a function which takes the "new" value of the
        watched attribute as the only parameter. If the callback return False,
        this cancels any further processing of the given event.

        Alternatively, the callback can be a string, in which case it means
        emitting the correspondingly-named event (i.e., connect to self)
        """
        if isinstance(slot, str):
            self._map.setdefault(signal, []).append(lambda x: self._emit(slot, x))
        else:
            self._map.setdefault(signal, []).append(slot)

    def _signal(self, event):
        """This is called by a an action on a widget

        Within an self.ignore_events context, nothing happens.

        Tests can execute this method by directly changing the values of
        widget components.
        """
        if self._ignoring_events:
            return
        name = event.name
        value = event.new
        logger.log(self._sigs.get(name, logging.DEBUG), f"{name}: {value}")
        self._emit(name, value)

    @contextlib.contextmanager
    def ignore_events(self):
        """Temporarily turn off events processing in this instance

        (does not propagate to children)
        """
        self._ignoring_events = True
        try:
            yield
        finally:
            self._ignoring_events = False

    def _emit(self, sig, value=None):
        """An event happened, call its callbacks

        This method can be used in tests to simulate message passing without
        directly changing visual elements.

        Calling of callbacks will halt whenever one returns False.
        """
        for callback in self._map.get(sig, []):
            if callback(value) is False:
                break

    def show(self, threads=False):
        """Open a new browser tab and display this instance's interface"""
        return pn.serve(self.widget, show=True, threaded=threads)

class SingleSelect(SigSlot):
    """A multiselect which only allows you to select one item for an event"""
    signals = ['_selected', 'selected']
    slots = ['set_options', 'set_selection', 'add', 'clear', 'select']

    def __init__(self, **kwargs):
        self.kwargs = kwargs
        super().__init__()

class FileSelector(SigSlot):
    """Panel-based graphical file selector widget

    Instances of this widget are interactive and can be displayed in jupyter by having
    them as the output of a cell,  or in a separate browser tab using ``.show()``.
    """
    signals = ['protocol_changed', 'selection_changed', 'directory_entered', 'home_clicked', 'up_clicked', 'go_clicked', 'filters_changed']
    slots = ['set_filters', 'go_home']

    def __init__(self, url=None, filters=None, ignore=None, kwargs=None):
        """

        Parameters
        ----------
        url : str (optional)
            Initial value of the URL to populate the dialog; should include protocol
        filters : list(str) (optional)
            File endings to include in the listings. If not included, all files are
            allowed. Does not affect directories.
            If given, the endings will appear as checkboxes in the interface
        ignore : list(str) (optional)
            Regex(s) of file basename patterns to ignore, e.g., "\\." for typical
            hidden files on posix
        kwargs : dict (optional)
            To pass to file system instance
        """
        if url:
            self.init_protocol, url = split_protocol(url)
        else:
            self.init_protocol, url = ('file', os.getcwd())
        self.init_url = url
        self.init_kwargs = (kwargs if isinstance(kwargs, str) else str(kwargs)) or '{}'
        self.filters = filters
        self.ignore = [re.compile(i) for i in ignore or []]
        self._fs = None
        super().__init__()

    @property
    def storage_options(self):
        """Value of the kwargs box as a dictionary"""
        try:
            return ast.literal_eval(self.init_kwargs)
        except (ValueError, SyntaxError):
            return {}

    @property
    def fs(self):
        """Current filesystem instance"""
        if self._fs is None:
            self._fs = filesystem(self.init_protocol, **self.storage_options)
        return self._fs

    @property
    def urlpath(self):
        """URL of currently selected item"""
        return f"{self.init_protocol}://{self.init_url}"

    def open_file(self, mode='rb', compression=None, encoding=None):
        """Create OpenFile instance for the currently selected item

        For example, in a notebook you might do something like

        .. code-block::

            [ ]: sel = FileSelector(); sel

            # user selects their file

            [ ]: with sel.open_file('rb') as f:
            ...      out = f.read()

        Parameters
        ----------
        mode: str (optional)
            Open mode for the file.
        compression: str (optional)
            The interact with the file as compressed. Set to 'infer' to guess
            compression from the file ending
        encoding: str (optional)
            If using text mode, use this encoding; defaults to UTF8.
        """
        return OpenFile(self.fs, self.urlpath, mode=mode, compression=compression, encoding=encoding)

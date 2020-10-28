# Contains functions to quickly generate a Latex table

from util.printer import *

def table_open(col_width, align='l', hline=True, vline=True, sep='\n'):
    return Tabler(col_width, align, hline, vline, sep)

class Tabler(object):
    '''Object to quickly generate Latex tables'''
    def __init__(self, col_width, align='l', hline=True, vline=True, sep='\n'):
        self.table = ''
        self._width = col_width
        self.align = align
        self.hline = hline
        self.vline = vline
        self.sep = sep
        self.head = 0

        self._caption = 'A caption'
        self._label = 'tab:tablename'

        if self._width <= 0:
            raise RuntimeError('Cannot make table for header length "{}"'.format(col_width))


    @property
    def width(self):
        return self._width

    @property
    def caption(self):
        return self._caption
    @caption.setter
    def set_caption(self, val):
        self._caption = str(val)

    @property
    def label(self):
        return self._label
    
    @label.setter
    def set_label(self, val):
        self._label = str(val)

    # Just appends a line, writing the separator
    def __append_line(self, line):
        self.table += line + self.sep

    # Appends a row of values
    def __append_row(self, vals, hline, check):
        if check:
            if self.head != 0:
                raise RuntimeError('Last element-write did not fill the previous row yet! Filled {}/{} column entries.'.format(self.head, self._width))
            if len(vals) != self._width:
                raise RuntimeError('Too little values supplied to construct a row: Need {}, provided {}: "{}"'.format(self._width, len(vals), vals))
        self.table += ' & '.join((str(x) for x in vals)) + ' \\\\' + (' \\hline' if hline else '') + self.sep

    def __append_elem(self, elem, hline):
        if self.head < self._width-1:
            self.table += str(elem) + ' & '
            self.head += 1
        else:
            self.table += str(elem) + ' \\\\' + (' \\hline' if hline else '') + self.sep
            self.head = 0


    def __enter__(self):
        self.__append_line('\\begin{table}[]')
        self.__append_line('\\centering')
        cols = ('|'+('|'.join((self.align for x in range(self._width))))+'|') if self.vline else ''.join((self.align for x in range(self._width)))
        self.__append_line('\\begin{tabular}{'+cols+'}'+(' \\hline' if self.hline else ''))
        return self

    # Write a line as a row, splitting on sep
    # check is recommended to be True, but removes laziness
    def write_line(self, line, sep=',', hline=True, check=True):
        self.__append_row(line.split(sep), hline, check)


    # Write elementwise to this object
    def write_elem(self, elem, hline=True):
        self.__append_elem(elem, hline)

    # Write multiple elements at once
    def write_elems(self, elems, hline=True):
        for elem in elems:
            self.write_elem(elem, hline)


    def __exit__(self, type, value, traceback):
        self.__append_line('\\end{tabular}')
        self.__append_line('\\caption{'+self._caption+'}')
        self.__append_line('\\label{'+self._label+'}')
        self.__append_line('\\end{table}')
        print(self.table)
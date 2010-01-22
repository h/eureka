# To use this module, htmltopdf from poppler needs to be installed and in your
# PATH. In Ubuntu, this is the package poppler-utils, and on windows, it can be
# downloaded from http://sourceforge.net/projects/pdftohtml/

from __future__ import with_statement
from tempfile import NamedTemporaryFile
from subprocess import Popen, PIPE

class PDFException(Exception): pass

def pdftohtml(fp, command='pdftohtml', xml=True, extra_args=[]):
    '''
    Uses pdftohtml to convert a file-like object fp into an xml or html file
    (depending on the value of the `xml` argument). *args are command-line
    arguments that are passed to the `pdftohtml` command.

    '''

    from lxml import etree
    from eureka.xml import XMLParser

    with NamedTemporaryFile(suffix='.pdf') as tempfile:
        tempfile.write(fp.read())

        cmdline = [command, '-stdout']
        if xml:
            cmdline.append('-xml')
        cmdline.extend(extra_args)
        cmdline.append(tempfile.name)

        proc = Popen(args=cmdline, stdout=PIPE)
        xml = etree.parse(proc.stdout, parser=XMLParser()).getroot()
        returncode = proc.wait()

        if returncode != 0:
            raise PDFException('pdftohtml was unable to convert file from pdf '
                               'to html. Return code was %s' % returncode)

        return xml


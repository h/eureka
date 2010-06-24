# To use this module, htmltopdf from poppler needs to be installed and in your
# PATH. In Ubuntu, this is the package poppler-utils, and on windows, it can be
# downloaded from http://sourceforge.net/projects/pdftohtml/

from tempfile import NamedTemporaryFile
from subprocess import Popen, PIPE

class PDFException(Exception): pass

def pdftohtml(fp, command=None, xml=True, extra_args=[]):
    '''
    Uses pdftohtml to convert a file-like object fp into an xml or html file
    (depending on the value of the `xml` argument). *args are command-line
    arguments that are passed to the `pdftohtml` command.

    '''

    from lxml import etree
    from eureka.xml import XMLParser

    if not command:
        from settings import pdf_converter
        command = pdf_converter

    tempfile = NamedTemporaryFile(suffix='.pdf', delete=False)
    tempfile.write(fp.read())
    tempfile.close()
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
    from os import remove
    remove(tempfile.name)


    return xml


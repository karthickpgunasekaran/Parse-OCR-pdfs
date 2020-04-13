from typing import Optional, Union, Tuple, List, Dict, Any
import pdfquery as pq
from pdfquery.cache import FileCache
from pdfquery.pdfquery import PDFQuery
from pyquery.pyquery import PyQuery
import re
from dataclasses import dataclass, asdict
from pdfminer.pdfinterp import resolve1
import logging
from pathlib import Path
from functools import partial
import xlsxwriter
import json
from pymongo import MongoClient
import itertools
import functools
logger = logging.getLogger(__name__)

LEVEL = logging.DEBUG
# LEVEL = logging.INFO

logging.basicConfig(
    format="%(levelname)s -  file: %(pdf_name)s - page: %(pdf_page)s - %(message)s",
    level=LEVEL)
_logging_extra = {'pdf_name': "", "pdf_page": ""}


def _set_filehandler(filename: str, mode: str = logging.DEBUG):
    fh = logging.FileHandler(filename)
    fh.setLevel(mode)
    form = logging.Formatter(
        "%(levelname)s -  file: %(pdf_name)s - page: %(pdf_page)s - %(message)s"
    )
    fh.setFormatter(form)
    logger.addHandler(fh)


class PDFQueryException(Exception):
    pass


class PDFEnd(PDFQueryException):
    pass


p_date_line = r"(?:(?:in\s+der\s+([\d\w]+)\. Sitzung )|)am\s+(\w+)\s+den\s+([\d\w]+)\.\s+(\w+)\s+([\d\w]+)"  # noqa
r_date_line = re.compile(p_date_line)
p_topic_end = r"Name"
r_topic_end = re.compile(p_topic_end)


@dataclass
class Date:
    weekday: Optional[str] = None
    day: Optional[int] = None
    month: Optional[str] = None
    year: Optional[int] = None

    @classmethod
    def from_re_match(cls, m: re.Match):
        logger.info("Creating Date object", extra=_logging_extra)
        g: Tuple[str] = m.groups()

        if len(g) < 5:
            raise PDFQueryException(
                "Number of groups ({}) not as expected ({})".format(len(g), 5))
        obj = cls(g[1], g[2], g[3], g[4])
        logger.info(str(obj), extra=_logging_extra)

        return obj


class MongoDBWritable(object):
    """ it should have id method which should add _id field"""

    def id(self):
        raise NotImplementedError

    def asdict(self) -> Dict:
        d = asdict(self)
        d['_id'] = self.id()

        return d


@dataclass
class RollCall(MongoDBWritable):
    number: Optional[int] = None
    date: Optional[Date] = None
    page: Optional[int] = None
    topic: Optional[str] = None
    filename: Optional[str] = None
    bbox: Optional[List] = None

    def id(self):
        if self.bbox is not None:
            bbox_string = json.dumps(self.bbox)
        else:
            bbox_string = str(self.bbox)
        elements = [
            str(self.filename),
            str(self.page), bbox_string,
            str(self.number)
        ]
        uuid = '_'.join(elements)

        return uuid


@dataclass
class NameData(MongoDBWritable):
    full_name: Optional[str] = None
    occupation: Optional[str] = None
    constituency: Optional[str] = None
    district: Optional[str] = None
    party: Optional[str] = None
    page: Optional[int] = None
    filename: Optional[str] = None
    match_number: Optional[int] = None

    def id(self):
        if self.full_name is None:
            raise ValueError("Name cannot be None")

        return self.full_name.strip()


class Writer:
    def write(self, rc: MongoDBWritable, **kwargs):
        raise NotImplementedError

    def close(self):
        pass


class MongoDB(Writer):
    def __init__(self,
                 host='localhost',
                 port=27017,
                 db_name=None,
                 collection_name=None):
        self.client = MongoClient(host, port)
        self.db_name = db_name
        self.collection_name = collection_name

    def write(self, rc: MongoDBWritable):
        col = self.client[self.db_name][self.collection_name]
        data = rc.asdict()
        key = data.pop('_id')
        logger.info("Writing to mongodb {}".format(col), extra=_logging_extra)

        res = col.update_one({'_id': key}, {'$set': data}, upsert=True)

        if not res.acknowledged:
            logger.error("Could not write to mongodb", extra=_logging_extra)


class XLWriter(object):
    def __init__(self, filename: str):
        self.filename = filename
        self.workbook = xlsxwriter.Workbook(self.filename)
        self.sheets: Dict[XLSheetWriter] = {}

    def add_sheet(self, name: str) -> xlsxwriter.worksheet.Worksheet:
        logger.info("Creating sheet {} in {}".format(name, self.filename))

        if name in self.sheets:
            logger.warn("Sheet with name {} already exists in {}".format(
                name, self.filename))

        self.sheets[name] = XLSheetWriter(self.workbook.add_worksheet(name))

        return self.sheets[name]

    def write_rollcall_to_sheet(self, rc: RollCall, sheet_name: str):
        logger.info("Writing to file {}".format(self.filename))
        sheet = self.sheets.get(sheet_name)

        if sheet is None:
            sheet = self.add_sheet(sheet_name)
        sheet.write_rollcall(rc)

    def close(self):
        self.workbook.close()


class XLSheetWriter(object):
    def __init__(self, sheet: xlsxwriter.worksheet.Worksheet):
        self.sheet = sheet
        self.row = 0
        self.col = 0
        self.write_headers()

    def write_headers(self):
        self.sheet.write_string(self.row, self.col, "Filename")
        self.col += 1
        self.sheet.write_string(self.row, self.col, "Page")
        self.col += 1
        self.sheet.write_string(self.row, self.col, "Date")
        self.col += 1
        self.sheet.write_string(self.row, self.col, "Topic")
        self.row += 1
        self.col = 0

    def write_rollcall(self, rc: RollCall):
        logger.info("Writing to sheet {}".format(self.sheet.name))
        self.sheet.write_string(self.row, self.col, rc.filename)
        self.col += 1
        self.sheet.write_number(self.row, self.col, rc.page)
        self.col += 1
        self.sheet.write_string(self.row, self.col, str(rc.date))
        self.col += 1
        self.sheet.write_string(self.row, self.col, rc.topic)
        self.row += 1
        self.col = 0


@dataclass
class Location:
    x0: float = 0.
    y0: float = 0.
    x1: float = 0.
    y1: float = 0.


def get_bbox(pq_obj: PyQuery) -> List:

    return json.loads(pq_obj.attr('bbox'))


def is_date(line: str):
    m = r_date_line.match(line)

    return m


def get_number_of_pages(pdf: PDFQuery):
    return resolve1(pdf.doc.catalog['Pages'])['Count']


def look_for_line(pdf: PDFQuery, line: str, regex=False) -> PyQuery:
    if not regex:

        pq_obj = pdf.pq('LTTextLineHorizontal:contains("{}")'.format(line))
     
    else:
        pq_obj = pdf.pq()

    return pq_obj


class File(object):
    def __init__(self, filename: str):
        self.filename = Path(filename)
        self.file = PDFQuery(self.filename)

    def page(self, number: int) -> Any:
        return self.file.load(number)

################################################################################################################
class Reader(object):
    look_for = "Namentliche Abstimmung"

    def reset(self):
        self.filename = None
        self.pdf = None
        self.num_pages = None
        self.current_page = -1

    def __init__(self,
                 check_next: int = 5,
                 max_topic_range: int = 100,
                 flush_mem_after: int = 5,
                 start_page: int = 0,
                 end_page: int = None,
                 writer: Writer = None,
                 log_file: Optional[str] = None,
                 err_file: Optional[str] = None):
        self.filename = None
        self.pdf = None
        self.num_pages = None
        self.current_page = -1
        self.check_next = check_next
        self.max_topic_range = max_topic_range
        self.rollcalls: List[RollCall] = []
        self.flush_mem_after = flush_mem_after
        self.start_page = start_page
        self.end_page = end_page
        self.page_iterator = None
        self.writer = writer
        self.log_file = log_file
        self.err_file = err_file
        self.log_file_set = False
########################################################
    def load_file(self):
        return PDFQuery(self.filename)
########################################################
    def setup_file(self, filename: str):
        logger.info("Loading file {}".format(filename), extra=_logging_extra)
        self.filename = filename
        self.pdf = self.load_file()
        self.num_pages = get_number_of_pages(self.pdf)
        start_page = self.start_page
        end_page = self.end_page or self.num_pages
        logger.info(
            "Page range : ({}, {})".format(start_page, end_page),
            extra=_logging_extra)
        self.page_iterator = range(start_page, end_page)
        self.current_page = start_page - 1
        # setup log file

        if not self.log_file_set:
            self.setup_logging_files(filename)
########################################################
    def setup_logging_files(self, filename: str):
        if self.log_file is not None:
            _set_filehandler(self.log_file, logging.DEBUG)

            if self.err_file is None:
                self.err_file = Path(self.log_file).with_suffix('.err')
                _set_filehandler(self.err_file, logging.ERROR)
            else:
                _set_filehandler(self.err_file, logging.ERROR)
            self.log_file_set = True

            return
        else:
            for suffix in itertools.count(1):
                prefix = (Path('.') / filename.stem)
                log_file = prefix.with_name(prefix.stem + '_' +
                                            str(suffix)).with_suffix('.log')

                if not log_file.exists():

                    _set_filehandler(log_file, logging.DEBUG)
                    err_file = log_file.with_suffix('.err')
                    _set_filehandler(err_file, logging.ERROR)

                    break
        logger.info(
            "Setting log file to {}".format(self.log_file.absolute()),
            extra=_logging_extra)
        logger.info(
            "Setting error file to {}".format(self.err_file.absolute()),
            extra=_logging_extra)

        # if self.writer is not None:
        #    self.writer.add_sheet(filename)
########################################################
    def next_page(self):
        self.current_page += 1

        if self.current_page >= self.num_pages:
            raise PDFEnd("Reached end of pdf")
        '''
        logger.debug(
            "New page.",
            extra={
                "pdf_page": self.current_page + 1,
                "pdf_name": self.filename
            })
        '''
        # need to recreate the PDFQuery object
        # because simply calling load()
        # on the next page, keeps the
        # previous page in memory

        if self.current_page % self.flush_mem_after == 0:
            #logger.info(
            #    "Releasing memory, reloading pdf", extra=_logging_extra)
            self.pdf = self.load_file()
        self.pdf.load(self.current_page)

        return self.pdf
########################################################
    def look_for_line(self,custom_str=None) -> PyQuery:
        if custom_str==None:
        	pq_obj = look_for_line(self.pdf, self.look_for)
        else:
           	pq_obj = look_for_line(self.pdf,custom_str)
        return pq_obj

########################################################
    def check_next_few(self,
                       pq_obj: PyQuery) -> Tuple[Optional[Date], re.Match]:
        date = None
        cursor = pq_obj.next()

        for i in range(self.check_next):
            m = is_date(cursor.text())

            if m:
                logger.info(
                    "Date found",
                    extra={
                        "pdf_page": self.current_page + 1,
                        "pdf_name": self.filename
                    })
                date = Date.from_re_match(m)

                break

            cursor = cursor.next()

        return date, m
########################################################
    def end_of_topic(self, text: str):
        return r_topic_end.search(text)
########################################################
    def extract_topic(self, pq_obj: PyQuery):
        cursor = pq_obj.next()
        # skip till date occurs
        date_found = False

        for i in range(self.check_next):
            if is_date(cursor.text()):
                date_found = True
                date_text = cursor.text()
                cursor = pq_obj.next()  # reset

                break
            cursor = cursor.next()

        if not date_found:
            logger.error(
                "Date not found. Extract should not have been called.",
                extra={
                    "pdf_page": self.current_page + 1,
                    "pdf_name": self.filename
                })
        # cursor is after date
        topic = []
        topic_ended = False

        for i in range(self.max_topic_range):
            if (cursor.text()) == date_text:
                cursor = cursor.next()

                continue

            t = cursor.text()

            if self.end_of_topic(t):
                topic_ended = True

                break
            topic.append(t)
            cursor = cursor.next()

        if not topic_ended:
            # breakpoint()
            err = "Topic did not end in {} lines".format(self.current_page)
            logger.error(
                err,
                extra={
                    "pdf_page": self.current_page + 1,
                    "pdf_name": self.filename
                })
            logger.debug(
                "Last line read for topic was : {}".format(t),
                extra={
                    "pdf_page": self.current_page + 1,
                    "pdf_name": self.filename
                })
            # raise PDFQueryException(err)

        topic_str = ' '.join(topic)

        if not topic_str.strip():
            logger.error(
                "Registerd empty topic",
                extra={
                    "pdf_page": self.current_page + 1,
                    "pdf_name": self.filename
                })

            return ""
        else:
            return topic_str

    expected_vol_min = 4500.0
    expected_vol_max = 6500.0
########################################################
    def matches_expected_bbox_volume(self, bbox: List):
        v = (bbox[2] - bbox[0]) * (bbox[3] - bbox[1])

        if self.expected_vol_min < v < self.expected_vol_max:
            return True
        else:
            return False
########################################################
    def process_page(self) -> None:
        pq_obj = self.look_for_line() #looks for the table topic
        pq_obj2 = self.look_for_line("Zusammenstellung.")

        if not pq_obj and not pq_obj2:
            '''
            logger.debug(
                "No '{}' found".format(self.look_for),
                extra={
                    "pdf_page": self.current_page + 1,
                    "pdf_name": self.filename
                })
            '''

            return
        '''
        logger.debug(
                "Found '{}'".format(self.look_for),
                extra={
                    "pdf_page": self.current_page + 1,
                    "pdf_name": self.filename
                })
        '''
        if not pq_obj:
            pq_obj = pq_obj2

        logger.info(
                    "'{}'".format(pq_obj.text()),
                    extra={
                        "pdf_page": self.current_page + 1,
                        "pdf_name": self.filename
                    })
        return	
        '''
        date, m = self.check_next_few(pq_obj)
        expected_vol = self.matches_expected_bbox_volume(get_bbox(pq_obj))

        if expected_vol:
            logger.debug(
                "Bounding Box matches expected volume",
                extra={
                    "pdf_page": self.current_page + 1,
                    "pdf_name": self.filename
                })
        else:
            logger.debug(
                "Bounding Box does not match expected vol",
                extra={
                    "pdf_page": self.current_page + 1,
                    "pdf_name": self.filename
                })

        if date is None:
            logger.debug(
                "No date after '{}'".format(self.look_for),
                extra={
                    "pdf_page": self.current_page + 1,
                    "pdf_name": self.filename
                })

            if expected_vol:
                logger.error(
                    "Expeceted volume matches but could not find date",
                    extra={
                        "pdf_page": self.current_page + 1,
                        "pdf_name": self.filename
                    })

            return

        meeting_number = m.group(1)
        # this is what we want
        topic = self.extract_topic(pq_obj)
        bbox = get_bbox(pq_obj)
        rc = RollCall(meeting_number, date, self.current_page, topic,
                      Path(self.filename).name, bbox)
        logger.info(
            "Created {}".format(rc),
            extra={
                "pdf_page": self.current_page + 1,
                "pdf_name": self.filename
            })
        # write if writer is given
	
        if self.writer is not None:
            self.writer.write(rc)
        self.rollcalls.append(rc)
        '''
################################### THis function is called first it processes entire file

    def read(self, filename: str):
        self.setup_file(filename)   # setup pdf file for readding 
        logger.info(
            "Reading file {}".format(self.filename), extra=_logging_extra)
        now = len(self.rollcalls)
	#process each page separately
        for page_no in self.page_iterator:
            self.next_page()   #setup next page
            self.process_page()  #process next page
        final = len(self.rollcalls)
        logger.info(
            "{} roll call votes read from file {}".format(
                final - now, filename),
            extra={
                "pdf_page": self.current_page + 1,
                "pdf_name": filename
            })

        self.reset()

        if self.writer:
            self.writer.close()


def _replace(inp: str, a: str, b: str):
    return inp.replace(a, b)
############################################################################################################################################################

class PageTextReader(Reader):
    """Converts each page into pure text"""

    def __init__(self,
                 check_next: int = 5,
                 max_topic_range: int = 100,
                 flush_mem_after: int = 5,
                 start_page: int = 0,
                 end_page: int = None,
                 writer: Writer = None,
                 log_file: Optional[str] = None,
                 err_file: Optional[str] = None,
                 replacements: List[Tuple[str, str]] = None):
        super().__init__(
            check_next=check_next,
            max_topic_range=max_topic_range,
            flush_mem_after=flush_mem_after,
            start_page=start_page,
            end_page=end_page,
            writer=writer,
            log_file=log_file,
            err_file=err_file)
        self.replacements = replacements or [('\xad', '')]

    def process_page(self):
        raise NotImplementedError

    def get_page_text(self):
        full_text = [
            functools.reduce(
                lambda s, repl_with: _replace(s, repl_with[0], repl_with[1]),
                self.replacements, elem.text) for elem in self.pdf.pq(
                    'LTTextBoxHorizontal, LTTextLineHorizontal')
        ]

        return '\n'.join(full_text)

    def read(self, filename: str):
        self.setup_file(filename)
        logger.info(
            "Reading file {}".format(self.filename), extra=_logging_extra)
        now = len(self.rollcalls)

        for page_no in self.page_iterator:
            self.next_page()
            self.process_page()
        final = len(self.rollcalls)
        logger.info(
            "{} items read from file {}".format(final - now, filename),
            extra={
                "pdf_page": self.current_page + 1,
                "pdf_name": filename
            })

        self.reset()

        if self.writer:
            self.writer.close()


class NamesReader(PageTextReader):
    names_pattern = r"^([^\n;\d]{1,200});([^—]{1,200})—([\s\w\n]{1,100})\."
    occ_consti_dist = r"(.+)\s+(Wahlkr\.\s+\d+)(.+)"

    @classmethod
    def valid_name(cls, name: str) -> bool:
        if ',' in name:
            return True
        else:
            return False

    def process_page(self):
        # get the text
        # breakpoint()
        text = self.get_page_text()
        # iterate over the matches
        matches = re.finditer(self.names_pattern, text, re.MULTILINE)
        matchNum = 0

        for matchNum, match in enumerate(matches, start=1):
            logger.debug(
                "Match {matchNum} was found at {start}-{end}: {match}".format(
                    matchNum=matchNum,
                    start=match.start(),
                    end=match.end(),
                    match=match.group()),
                extra={
                    "pdf_page": self.current_page + 1,
                    "pdf_name": self.filename
                })
            name = ' '.join(match.group(1).split())

            if not self.valid_name(name):
                logger.info(
                    "{name} of match number {mn} not a valid name".format(
                        name=name, mn=matchNum),
                    extra={
                        "pdf_page": self.current_page + 1,
                        "pdf_name": self.filename
                    })

                continue

            occupation_district = ' '.join(match.group(2).split())
            m = re.match(self.occ_consti_dist, occupation_district)

            if not m:
                logger.error(
                    "{occ_dist} not in '<occupation> Wahlkr. <number> (<district>)' form"
                    .format(occ_dist=occupation_district),
                    extra={
                        "pdf_page": self.current_page + 1,
                        "pdf_name": self.filename
                    })
                occ = occupation_district
                consti = ''
                dist = ''
            else:
                try:
                    occ = m.group(1)
                    consti = m.group(2)
                    dist = m.group(3)
                except IndexError as ie:
                    logger.error(
                        "Occupation district regex groups don't match",
                        extra={
                            "pdf_page": self.current_page + 1,
                            "pdf_name": self.filename
                        })
                    occ = occupation_district
                    consti = ''
                    dist = ''

            party = ' '.join(match.group(3).split())

            data_instance = NameData(
                full_name=name,
                occupation=occ,
                constituency=consti,
                district=dist,
                party=party,
                page=self.current_page,
                filename=self.filename,
                match_number=matchNum)
            self.rollcalls.append(data_instance)
            self.writer.write(data_instance)
        if matchNum>0:    
            logger.info(
            "Found {} names on this page.".format(matchNum),
            extra={
                "pdf_page": self.current_page + 1,
                "pdf_name": self.filename
            })

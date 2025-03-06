import json
import cgi
import urllib
import os
import sys
import hashlib
import datetime
import random
import pybio
import pubscan
import glob
import shlex
import copy
import re
import shutil
import yaml
from unidecode import unidecode
import requests
import urllib3
import xmltodict
import pickle
from itertools import combinations
import re
import html
urllib3.disable_warnings()
from operator import itemgetter
from sqlalchemy import *
from sqlalchemy.orm import registry, relationship, backref, validates, sessionmaker, scoped_session
from sqlalchemy.ext.declarative import declarative_base

pubscan_folder = os.path.dirname(os.path.realpath(__file__))
config = yaml.safe_load(open(os.path.join(pubscan_folder, "pubscan.config.yaml")))

data_folder = os.path.dirname(os.path.realpath(__file__))
data_folder = os.path.join(data_folder, "data")

def dthandler(datetime_object):
    if isinstance(datetime_object, datetime.datetime):
        return "%04g/%02g/%02g %02g:%02g" % (datetime_object.year, datetime_object.month, datetime_object.day, datetime_object.hour, datetime_object.minute)
    if isinstance(datetime_object, datetime.date):
        return "%04g/%02g/%02g" % (datetime_object.year, datetime_object.month, datetime_object.day)
    if isinstance(datetime_object, datetime.timedelta):
        hours = datetime_object.seconds/3600
        minutes = (datetime_object.seconds - hours*3600)/60
        seconds = datetime_object.seconds - hours*3600 - minutes*60
        return "%02gh:%02gm:%02gs" % (hours, minutes, seconds)

engine = create_engine(f'mysql://{config["mysql"]["username"]}:{config["mysql"]["password"]}@localhost/{config["mysql"]["database"]}', pool_size = 1, pool_recycle=5)

Session = scoped_session(sessionmaker(bind=engine))

meta = MetaData()
meta.reflect(bind=engine, views=True)

mapper_registry = registry()

def create_json(results, records="", status=""):
    r = {}
    r["records"] = len(results) if records=="" else records
    r["status"] = status
    data = []
    for result in results:
        data.append(result.get_json())
    r["data"] = data
    return json.dumps(r, default=dthandler)

class Basic(object):
    def get_json(self):
        d = {}
        for j in self.__dict__.keys():
            if j in ["_sa_instance_state"]:
                continue
            d[j] = self.__dict__[j]
        return d    

class Users(Basic):
    pass

users_table = meta.tables["Users"]
mapper_registry.map_imperatively(Users, users_table)

#import stripe
#stripe.api_key = 'sk_test_1iVkPyJIyvEOzGfTIwNj978V00foajlMFx'

def remove_special_characters(text):
    return unidecode(text)
    #return re.sub(r'[^a-zA-Z0-9\s]', '', text)

def get_full_name(author):
    fore_name = unidecode(author.get("ForeName", ""))
    last_name = unidecode(author.get("LastName", ""))
    suffix = unidecode(author.get("Suffix", ""))
    initials = unidecode(author.get("Initials", ""))
    if not fore_name and initials:  
        fore_name = initials
    full_name = f"{fore_name} {last_name}".strip()
    if suffix:
        full_name += f", {suffix}"
    return full_name

def data_author_to_pmids(search, nocache=None):
    search_id = search.replace(" ", "_").lower()
    search_id = remove_special_characters(search_id)
    search_file = os.path.join(data_folder, f"{search_id}.author_search")

    if os.path.exists(search_file) and nocache==None:
        with open(search_file, 'rb') as file:
            result = pickle.load(file)
    else:
        url = f"https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi?term={search}[Author]&retmax=200&retmode=json"
        page = requests.get(url, verify=False).text
        result = json.loads(page)["esearchresult"]
        with open(search_file, 'wb') as file:
            pickle.dump(result, file)
    return result

def data_pmid(pmid, nocache=None):
    pmid_file = os.path.join(data_folder, f"{pmid}.pmid")

    if os.path.exists(pmid_file) and nocache==None:
        with open(pmid_file, 'rb') as file:
            myxml = pickle.load(file)
    else:
        url = f"https://eutils.ncbi.nlm.nih.gov/entrez/eutils/efetch.fcgi?db=pubmed&id={pmid}&retmode=xml"
        page = requests.get(url, verify=False)
        page.encoding = 'utf-8'
        response = page.text
        myxml = xmltodict.parse(response)
        with open(pmid_file, 'wb') as file:
            pickle.dump(myxml, file)            

    authors = myxml["PubmedArticleSet"]["PubmedArticle"]["MedlineCitation"]["Article"]["AuthorList"]["Author"]
    authors_t = []
    for rec in authors:
        #t = f'{unidecode(rec["ForeName"])} {unidecode(rec["LastName"])}'
        t = get_full_name(rec)
        authors_t.append(t)
    return authors_t

class TableClass():

    def return_string(self, cont):
        return cont.encode("utf-8")

    def log(self, message):
        print(message, file=self.environ['wsgi.errors'])

    def __init__(self, environ, start_response):
        self.environ = environ
        self.start = start_response
        self.get_done = "Get done."
        self.string_remove = "Remove done."
        self.string_put = "Save done."
        self.string_insert = "Save done."
        self.string_save = "Save done."
        self.string_key_conflict = "Key conflict."
        self.pars = self.parse_fields(self.environ)
        self.username = self.pars.get("username", "public")
        self.password = self.pars.get("password", "public")
        self.db = {}

    def __iter__(self):
        status = '200 OK'
        response_type = self.pars.get("response_type", None)
        if response_type in ["plain", None]:
            response_headers = [('Content-type','text/plain; charset=utf-8')]
        elif response_type in ["json"]:
            response_headers = [('Content-type','application/json; charset=utf-8')]
        self.stream_out = self.start(status, response_headers)
        method = getattr(self, self.pars.get("action", "version"))
        yield method()

    def parse_fields(self, environ):
        request_method = environ["REQUEST_METHOD"]
        if environ["REQUEST_METHOD"]=="GET":
            pars = urllib.parse.parse_qs(environ['QUERY_STRING'])
            for par, [val] in pars.items():
                pars[par] = val
        if environ["REQUEST_METHOD"]=="POST":
            self.formdata = cgi.FieldStorage(environ=environ, fp=environ['wsgi.input'])
            pars = {}
            for key in self.formdata.keys():
                  pars[key] = self.formdata[key].value
        return pars

    def version(self):
        user_count = Session.query(Users).count()
        status_string = f"""pubscan v0.1 {datetime.datetime.now()}
Database: {config["mysql"]["database"]}
Users: {user_count}
"""
        return self.return_string(status_string)

    def config(self):
        return self.return_string(config["folders"]["library_folder"])

    def get_author_network(self):
        search = unidecode(self.pars["author"])
        nocache = self.pars.get("nocache", None)
        result = data_author_to_pmids(search, nocache)

        author_pmids = {}
        author_author_pmids = {}
        nodes_all = []
        edges_all = []
        authors = set()

        for pmid in result["idlist"]:
            pmid_authors = data_pmid(pmid)
            for author_name in pmid_authors:
                authors.add(author_name)
                author_rec = { "id": author_name, "label": author_name, "group": "g1", "size": "15"};
                if author_rec not in nodes_all:
                    nodes_all.append(author_rec)

            for author_name in pmid_authors:
                author_pmids[author_name] = data_author_to_pmids(author_name)["idlist"]

        # pairs of authors, number of overlapping pmids
        author_copmids = {}
        author_pairs = list(combinations(authors, 2))
        for a1, a2 in author_pairs:
            p1 = author_pmids[a1]
            p2 = author_pmids[a2]
            common = list(set(p1).intersection(p2))
            if len(common)>0:
                author_author_pmids[f"{a1}_{a2}"] = common
                #author_author_pmids[f"{a2}_{a1}"] = common
                edge_rec = {"from":a1, "to":a2, "width":len(common)/2, "length":100, "label": f"{len(common)}", "color": {"color": '#CB80AB', "highlight": '#CB80AB'} }
                edges_all.append(edge_rec)

        #result = json.dumps(result)
        #return self.return_string(result)

        results = {}
        #results["author_author_pmids"] = author_author_pmids
        results["nodes_all"] = nodes_all
        results["edges_all"] = edges_all

        return self.return_string(json.dumps(results))

    def get_pmid(self):
        pmid = self.pars["pmid"]
        nocache = self.pars.get("nocache", None)
        result = data_pmid(pmid, nocache)

        result = json.dumps(result)
        return self.return_string(result)
        #return self.return_string(str(authors))
        #return self.return_string(json.dumps(page, indent=2))

    def test_sql(self):
        conn = Session()
        u = Users()
        u.UserId = "gregor.rot@ethz.ch"
        conn.add(u)
        try:
            conn.commit()
            conn.flush()        
        except:
            pass
        return self.return_string("done")

    # find is case insensitive, replace is case sensitive (doesn't change the original text)
    # wraps s1 with up and down -> up + s1 + down, and replaces the construct in text
    def replace_ignorecase(self, s1, up, down, text):
        new_string = ""
        last_x = 0
        for x in [m.start() for m in re.finditer(s1, text, flags=re.IGNORECASE)]:
            new_string += text[last_x:x]
            new_string += up
            new_string += text[x:x+len(s1)]
            new_string += down
            last_x = x+len(s1)
        new_string += text[last_x:]
        return new_string

    def upload_file(self):
        if 'newfile' in self.formdata and self.formdata['newfile'].filename!='' and 'newfile2' in self.formdata and self.formdata['newfile2'].filename!='':
            seq_type = "paired"
        elif 'newfile' in self.formdata and self.formdata['newfile'].filename!='':
            seq_type = "single"
        else:
            seq_type = "none"

        if seq_type=="single":
            file_data = self.formdata['newfile'].file.read()
            filename = self.formdata['newfile'].filename
            lib_id = self.pars.get("lib_id", None)
            email = self.pars.get("email", None)
            chk_upload_email = self.pars.get("chk_upload_email", None)
            library = apa.annotation.libs[lib_id]
            if lib_id==None:
                return
            exp_id = library.add_empty_experiment(filename_R1=os.path.basename(filename))
            exp_folder = os.path.join(apa.path.data_folder, lib_id, "e"+str(exp_id))
            library.save()
            os.makedirs(exp_folder)
            if filename.endswith(".bz2"):
                target = os.path.join(exp_folder, "%s_e%s.fastq.bz2" % (lib_id, exp_id))
                f = open(target, 'wb')
                f.write(file_data)
                f.close()
                # convert bz2 to gz in case of bz2 upload
                self.add_ticket(email, "bunzip2 "+target, "bunzip2 " + "%s_e%s.fastq.bz2" % (lib_id, exp_id) + " to convert it to gzip format")
                self.add_ticket(email, "gzip "+target[:-3], "gzip " + "%s_e%s.fastq" % (lib_id, exp_id))
            if filename.endswith(".gz"):
                target = os.path.join(exp_folder, "%s_e%s.fastq.gz" % (lib_id, exp_id))
                f = open(target, 'wb')
                f.write(file_data)
                f.close()
            self.add_ticket(email, "apa.map -lib_id %s -exp_id %s -cpu 4" % (lib_id, exp_id), "map e%s (library %s) to reference genome %s" % (exp_id, lib_id, library.genome))
            self.add_ticket(email, "apa.map.stats -lib_id %s -exp_id %s" % (lib_id, exp_id), "map statistics for e%s (library %s)" % (exp_id, lib_id))
            self.add_ticket(email, "apa.fastqc /home/gregor/apa/data.apa/%s %s" % (lib_id, lib_id), "fastqc for library %s" % (lib_id))
            self.add_ticket(email, "apa.bed.gene_expression -lib_id %s" % lib_id, "gene expression table for library %s" % lib_id)
            self.add_ticket(email, "apa.bed.multi -lib_id %s" % (lib_id), "bed files for library %s" % lib_id)
            self.add_ticket(email, "apa.polya.makeconfig -lib_id %s" % (lib_id), "polya make config database for library %s" % lib_id)
            self.add_ticket(email, "apa.polya -poly_id %s" % (lib_id), "polya database for library %s" % lib_id)
            self.add_ticket(email, "apa.bed.polya_expression -lib_id %s -poly_id %s" % (lib_id, lib_id), "polya expression table for library %s" % lib_id)
            self.add_ticket(email, "apa.map.salmon -lib_id %s" % (lib_id), "salmon for library %s" % lib_id)
            if chk_upload_email=="on":
                self.add_ticket(email, "/home/gregor/expressrna.sendemail %s '%s'" % (email, "Dear %s,\n\nyour experiment e%s (library %s) has been mapped and processed now, you can access it here:\n\nhttp://expressrna.org/index.html?action=library&library_id=%s\n\nThank you,\nexpressRNA" % (email, exp_id, lib_id, lib_id)), "email processing done for e%s (library %s)" % (exp_id, lib_id))
        if seq_type=="paired":
            file_data_R1 = self.formdata['newfile'].file.read()
            filename_R1 = self.formdata['newfile'].filename
            file_data_R2 = self.formdata['newfile2'].file.read()
            filename_R2 = self.formdata['newfile2'].filename
            lib_id = self.pars.get("lib_id", None)
            email = self.pars.get("email", None)
            chk_upload_email = self.pars.get("chk_upload_email", None)
            library = apa.annotation.libs[lib_id]
            if lib_id==None:
                return
            exp_id = library.add_empty_experiment(filename_R1=os.path.basename(filename_R1), filename_R2=os.path.basename(filename_R2))
            exp_folder = os.path.join(apa.path.data_folder, lib_id, "e"+str(exp_id))
            library.save()
            os.makedirs(exp_folder)
            if filename_R1.endswith(".bz2"):
                target = os.path.join(exp_folder, "%s_e%s_R1.fastq.bz2" % (lib_id, exp_id))
                f = open(target, 'wb')
                f.write(file_data_R1)
                f.close()
            if filename_R1.endswith(".gz"):
                target = os.path.join(exp_folder, "%s_e%s_R1.fastq.gz" % (lib_id, exp_id))
                f = open(target, 'wb')
                f.write(file_data_R1)
                f.close()
                self.add_ticket(email, "gunzip "+target, "gunzip " + "%s_e%s_R1.fastq.gz" % (lib_id, exp_id) + " to convert it to bz2 format")
                self.add_ticket(email, "bzip2 "+target[:-3], "bzip2 " + "%s_e%s_R1.fastq" % (lib_id, exp_id))
            if filename_R2.endswith(".bz2"):
                target = os.path.join(exp_folder, "%s_e%s_R2.fastq.bz2" % (lib_id, exp_id))
                f = open(target, 'wb')
                f.write(file_data_R2)
                f.close()
            if filename_R2.endswith(".gz"):
                target = os.path.join(exp_folder, "%s_e%s_R2.fastq.gz" % (lib_id, exp_id))
                f = open(target, 'wb')
                f.write(file_data_R2)
                f.close()
                self.add_ticket(email, "gunzip "+target, "gunzip " + "%s_e%s_R2.fastq.gz" % (lib_id, exp_id) + " to convert it to bz2 format")
                self.add_ticket(email, "bzip2 "+target[:-3], "bzip2 " + "%s_e%s_R2.fastq" % (lib_id, exp_id))
            """
            self.add_ticket(email, "apa.map -lib_id %s -exp_id %s -cpu 4" % (lib_id, exp_id), "map e%s (library %s) to reference genome %s" % (exp_id, lib_id, library.genome))
            self.add_ticket(email, "apa.map.stats -lib_id %s -exp_id %s" % (lib_id, exp_id), "map statistics for e%s (library %s)" % (exp_id, lib_id))
            self.add_ticket(email, "apa.fastqc /home/gregor/apa/data.apa/%s %s" % (lib_id, lib_id), "fastqc for library %s" % (lib_id))
            self.add_ticket(email, "apa.bed.gene_expression -lib_id %s" % lib_id, "gene expression table for library %s" % lib_id)
            self.add_ticket(email, "apa.bed.multi -lib_id %s" % (lib_id), "bed files for library %s" % lib_id)
            self.add_ticket(email, "apa.polya.makeconfig -lib_id %s" % (lib_id), "polya make config database for library %s" % lib_id)
            self.add_ticket(email, "apa.polya -poly_id %s" % (lib_id), "polya database for library %s" % lib_id)
            self.add_ticket(email, "apa.bed.polya_expression -lib_id %s -poly_id %s" % (lib_id, lib_id), "polya expression table for library %s" % lib_id)
            if chk_upload_email=="on":
                self.add_ticket(email, "/home/gregor/expressrna.sendemail %s '%s'" % (email, "Dear %s,\n\nyour experiment e%s (library %s) has been mapped and processed now, you can access it here:\n\nhttp://expressrna.org/index.html?action=library&library_id=%s\n\nThank you,\nexpressRNA" % (email, exp_id, lib_id, lib_id)), "email processing done for e%s (library %s)" % (exp_id, lib_id))
            """
        return self.return_string("done")

    def close(self):
        Session.remove()

application = TableClass

"""
    def get(self):
        conn = Session()
        search = self.pars.get("search", "").replace(" ", "")
        species = self.pars.get("species", "hg19")
        loc = search.split(":")
        if len(loc)==2:
            chr = loc[0].lstrip("chr")
            pos = loc[1].split("-")
            if len(pos)!=2:
                pos = loc[1].split("..")
            if len(pos)==2:
                try:
                    pos_from, pos_to = int(pos[0]), int(pos[1])
                    q = conn.query(Apadb).filter(Apadb.species==species).filter(and_(Apadb.chr==chr, Apadb.pos>=pos_from, Apadb.pos<=pos_to)).all()
                except:
                    pass
        else:
            q = conn.query(Apadb).filter(Apadb.species==species).filter(or_(Apadb.gene_id.like("%%%s%%" % search), Apadb.gene_name.like("%%%s%%" % search))).all()
        result = {'status': 'done', 'data': []}
        for rec in q:
            result['data'].append(rec.get_json())
        result['records'] = len(result['data'])
        return self.return_string(str(json.dumps(result)))

    def get_analysis_status(self):
        analysis_id = self.pars.get("analysis_id", None)
        if analysis_id!=None:
            analysis = apa.comps.Comps(analysis_id)
            output = analysis.status
            return self.return_string(analysis.status)
        return self.return_string("")

    def list_analysis(self):
        email = self.pars.get("email", "public")
        email = self.check_login(email)
        sort_by, sort_order = self.pars.get("sort_by", "name:asc").split(":")
        current_page = int(self.pars.get("current_page", 0))
        records_per_page = int(self.pars.get("records_per_page", 5))
        search = self.pars.get("search", "").lower()
        search = search.split("|||")
        conn = Session()
        result = []
        config_files = glob.glob("/home/gregor/apa/data.comps/*/*.config")
        for fname in config_files:
            comps_id = os.path.basename(fname).replace(".config", "")
            try:
                comps = apa.comps.Comps(comps_id)
            except:
                comps = None
                continue
            last_change = os.stat(fname).st_mtime
            r = {}
            r["config_file"] = fname
            r["comps_id"] = comps.comps_id
            r["analysis_id"] = comps.comps_id
            r["comps_name"] = comps.name
            r["name"] = comps.name
            r["comps_name_search"] = comps.name
            r["last_change"] = datetime.datetime.fromtimestamp(last_change)
            r["notes"] = comps.notes
            r["notes_search"] = comps.notes
            r["authors"] = comps.authors
            r["authors_search"] = comps.authors
            r["method"] = comps.method
            r["method_search"] = db["methods"][comps.method]["desc"]
            r["genome"] = comps.genome
            r["status"] = comps.status
            r["analysis_type"] = comps.analysis_type
            r["genome_search"] = db["genomes"][comps.genome]["desc"]
            if r["method_search"]!="not selected":
                r["method_search"] = r["method_search"] % (db["methods"][r["method"]]["link"])
            if r["genome_search"]!="not selected":
                r["genome_search"] = r["genome_search"] % (db["genomes"][r["genome"]]["link_assembly"], db["genomes"][r["genome"]]["link_annotation"])
            include_analysis = True
            for term in search:
                if term=="":
                    continue
                include_analysis = False
                if comps.name.lower().find(term)!=-1 or comps.notes.lower().find(term)!=-1 or ",".join(comps.authors).lower().find(term)!=-1 or comps.comps_id.lower().find(term)!=-1 or r["method_search"].lower().find(term)!=-1 or r["genome_search"].lower().find(term)!=-1 or ",".join(r["authors_search"]).lower().find(term)!=-1:
                    include_analysis = True
                    r["comps_name_search"] = self.replace_ignorecase(term, "<div style='display: inline; font-weight: bold; color: #FF0000'>", "</div>", self.remove_links(r["comps_name_search"]))
                    r["notes_search"] = self.replace_ignorecase(term, "<div style='display: inline; font-weight: bold; color: #FF0000'>", "</div>", self.remove_links(r["notes_search"]))
                    r["authors_search"] = self.replace_ignorecase(term, "<div style='display: inline; font-weight: bold; color: #FF0000'>", "</div>", self.remove_links(", ".join(r["authors_search"])))
                    if r["method_search"]!="not selected":
                        r["method_search"] = self.replace_ignorecase(term, "<div style='display: inline; font-weight: bold; color: #FF0000'>", "</div>", self.remove_links(r["method_search"]))
                    if r["genome_search"]!="not selected":
                        r["genome_search"] = self.replace_ignorecase(term, "<div style='display: inline; font-weight: bold; color: #FF0000'>", "</div>", self.remove_links(r["genome_search"]))
            if (email in comps.access) or ("public" in comps.access) or (email in "gregor.rot@gmail.com"):
                if include_analysis:
                    result.append(r)

        result = sorted(result, key=itemgetter(sort_by))
        count = len(result)
        if sort_order=="desc":
            result.reverse()
        result = result[current_page * records_per_page: current_page * records_per_page + records_per_page]
        return self.return_string(json.dumps({"data":result, "count":count}, default=dthandler))

    def list_libraries(self):
        apa.annotation.init()
        email = self.pars.get("email", "public")
        email = self.check_login(email)
        current_page = int(self.pars.get("current_page", 0))
        records_per_page = int(self.pars.get("records_per_page", 5))
        search = self.pars.get("search", "").lower()
        search = search.split("|||")
        sort_by, sort_order = self.pars.get("sort_by", "name:asc").split(":")
        conn = Session()
        result = []
        libs = apa.annotation.libs
        for lib_id, lib_data in libs.items():
            if (not "public" in lib_data.access) and (not email in lib_data.access) and (email!="gregor.rot@gmail.com"):
                continue
            r = {}
            r["lib_id"] = lib_id
            r["lib_id_search"] = lib_id
            r["name"] = lib_data.name
            r["name_search"] = lib_data.name
            r["notes"] = lib_data.notes
            r["notes_search"] = lib_data.notes
            r["tags"] = lib_data.tags
            r["tags_search"] = lib_data.tags
            r["method"] = lib_data.method
            r["method_search"] = db["methods"][lib_data.method]["desc"]
            if (r["method_search"])!="not selected":
                r["method_search"] = r["method_search"] % db["methods"][lib_data.method]["link"]
            r["genome"] = lib_data.genome
            r["num_experiments"] = len(lib_data.experiments)
            r["genome_search"] = db["genomes"][lib_data.genome]["desc"]
            if (r["genome_search"])!="not selected":
                r["genome_search"] = r["genome_search"] % (db["genomes"][r["genome"]]["link_assembly"], db["genomes"][r["genome"]]["link_annotation"])
            include_library = True
            for term in search:
                if term=="":
                    continue
                include_library = False
                if r["tags_search"].lower().find(term)!=-1 or r["lib_id"].lower().find(term)!=-1 or r["name"].lower().find(term)!=-1 or r["notes"].lower().find(term)!=-1 or r["method_search"].lower().find(term)!=-1 or r["genome_search"].lower().find(term)!=-1:
                    include_library = True
                    r["lib_id_search"] = self.replace_ignorecase(term, "<div style='display: inline; font-weight: bold; color: #FF0000'>", "</div>", r["lib_id_search"])
                    r["name_search"] = self.replace_ignorecase(term, "<div style='display: inline; font-weight: bold; color: #FF0000'>", "</div>", self.remove_links(r["name_search"]))
                    r["notes_search"] = self.replace_ignorecase(term, "<div style='display: inline; font-weight: bold; color: #FF0000'>", "</div>", self.remove_links(r["notes_search"]))
                    r["tags_search"] = self.replace_ignorecase(term, "<div style='display: inline; font-weight: bold; color: #FF0000'>", "</div>", self.remove_links(r["tags_search"]))
                    if r["method_search"]!="not selected":
                        r["method_search"] = self.replace_ignorecase(term, "<div style='display: inline; font-weight: bold; color: #FF0000'>", "</div>", self.remove_links(r["method_search"]))
                    if r["genome_search"]!="not selected":
                        r["genome_search"] = self.replace_ignorecase(term, "<div style='display: inline; font-weight: bold; color: #FF0000'>", "</div>", self.remove_links(r["genome_search"]))
            if include_library:
                result.append(r)

        result = sorted(result, key=itemgetter(sort_by))
        count = len(result)
        if sort_order=="desc":
            result.reverse()
        result = result[current_page * records_per_page: current_page * records_per_page + records_per_page]
        return self.return_string(json.dumps({"data":result, "count":count}, default=dthandler))

    def remove_links(self, text):
        pattern =r'<(a|/a).*?>'
        return re.sub(pattern , "", text)

    def get_analysis(self):
        apa.annotation.init()
        stats = {}
        involved_libs = set()

        def read_stats(lib_id):
            if stats.get(lib_id, None)==None:
                fname = os.path.join(apa.path.lib_folder(lib_id), "%s_m%s.stats.tab" % (lib_id, 1))
                res = {}
                f = open(fname, "rt")
                r = f.readline()
                r = f.readline()
                while r:
                    r = r.replace("\r", "").replace("\n", "").split("\t")
                    res[int(r[0])] = (r[-3], r[-1])
                    r = f.readline()
                f.close()
                stats[lib_id] = res

        def make_table(comps_data):
            res = []
            for (cid, exp_list, name) in comps_data:
                for exp_rec in exp_list:
                    lib_id = "_".join(exp_rec.split("_")[:-1])
                    involved_libs.add(lib_id)
                    exp_id = int(exp_rec.split("_")[-1][1:])
                    ann = copy.copy(apa.annotation.libs[lib_id].experiments[exp_id])
                    ann["method_desc"] = ann["method"]
                    ann["lib_id"] = lib_id
                    ann["exp_id"] = exp_id
                    ann["cid"] = cid
                    read_stats(lib_id)
                    ann["stats"] = copy.copy(stats[lib_id][exp_id])
                    res.append(ann)
            return res

        comps_id = self.pars.get("comps_id", None)
        if comps_id==None:
            return self.return_string("empty")
        comps = apa.comps.Comps(comps_id)
        email = self.pars.get("email", "public")
        pair_type = self.pars.get("pair_type", "same")

        folder = os.path.join(apa.path.comps_folder, comps_id)
        last_change = os.stat(folder).st_mtime
        r = {}
        r["comps_id"] = comps_id
        r["comps_name"] = comps.name
        r["analysis_type"] = comps.analysis_type
        r["CLIP"] = comps.CLIP
        r["site_selection"] = comps.site_selection
        r["significance_thr"] = comps.significance_thr
        r["cDNA_thr"] = comps.cDNA_thr
        r["presence_thr"] = comps.presence_thr
        r["last_change"] = datetime.datetime.fromtimestamp(last_change)
        r["notes"] = comps.notes
        r["control"] = make_table(comps.control)
        r["test"] = make_table(comps.test)
        r["polya_db"] = comps.polya_db
        r["access"] = comps.access
        r["owner"] = comps.owner
        r["authors"] = comps.authors
        r["status"] = comps.status

        r["genome_desc"] = db["genomes"][comps.genome]["desc"]
        if r["genome_desc"]!="not selected":
            r["genome_desc"] = db["genomes"][comps.genome]["desc"] % (db["genomes"][comps.genome]["link_assembly"], db["genomes"][comps.genome]["link_annotation"])
        r["method"] = comps.method
        r["method_desc"] = db["methods"][comps.method]["desc"]
        if r["method_desc"]!="not selected":
            r["method_desc"] = db["methods"][comps.method]["desc"] % db["methods"][comps.method]["link"]

        # if experiments of the analysis come from various libraries, create a consensus library columns (annotation)
        columns = []
        involved_libs = list(involved_libs)
        for lib_id in involved_libs:
            lib = apa.annotation.libs[lib_id]
            for column in lib.columns:
                if column not in columns:
                    if column[1] not in ["method", "map_to"]:
                        columns.append(column)
        r["columns"] = columns

        go = {} # read GO files
        for aspect in ["P", "C"]:
            for reg_type in ["enhanced", "repressed"]:
                fname = os.path.join(apa.path.comps_folder, comps_id, "rnamap", "go_%s_%s_%s.json" % (reg_type, pair_type, aspect))
                if os.path.exists(fname):
                    go["%s_%s_%s" % (reg_type, pair_type, aspect)] = json.loads(open(fname, "rt").readline())
                else:
                    go["%s_%s_%s" % (reg_type, pair_type, aspect)] = []
                for site_type in ["proximal", "distal"]:
                    fname = os.path.join(apa.path.comps_folder, comps_id, "rnamap", "go_%s_%s_%s_%s.json" % (reg_type, site_type, pair_type, aspect))
                    if os.path.exists(fname):
                        go["%s_%s_%s_%s" % (reg_type, site_type, pair_type, aspect)] = json.loads(open(fname, "rt").readline())
                    else:
                        go["%s_%s_%s_%s" % (reg_type, site_type, pair_type, aspect)] = []
        r["go"] = go
        return self.return_string(json.dumps(r, default=dthandler))

    def get_ep(self):
        lib_id = self.pars.get("lib_id", None)
        genes = self.pars.get("genes", None)
        initialize = self.pars.get("initialize", None)
        if lib_id==None:
            return self.return_string("")
        fname = os.path.join(apa.path.lib_folder(lib_id), "%s_gene_expression_cpm.tab" % (lib_id))
        result = []
        res = []
        if genes!=None:
            parsed_genes = []
            genes = genes.split("|||")
            for gene in genes:
                gene = gene.split(", ")
                gene = gene[0]
                parsed_genes.append(gene)
            parsed_genes = '|'.join(parsed_genes)
            res, _ = pybio.utils.Cmd("grep -iE '%s' %s" % (parsed_genes, fname)).run()
            res = res.split("\n")[:-1]
        if initialize=="yes":
            res, _ = pybio.utils.Cmd("head -n 6 %s" % (fname)).run()
            res = res.split("\n")[1:-1]
        # get header, we want values for experiments in a dictionary by experiment id
        header, _ = pybio.utils.Cmd("head -n 1 %s" % (fname)).run()
        header = header.split("\n")[0].split("\t")
        for line in res[:10]: # max results
            line = dict(zip(header, line.split("\t")))
            result.append(line)
        return self.return_string(json.dumps(result, default=dthandler))

    def get_keywords_genes(self):
        lib_id = self.pars.get("lib_id", None)
        kw = self.pars.get("kw", "")
        if len(kw)<2:
            return json.dumps([])
        if lib_id==None:
            return ""
        fname = os.path.join(apa.path.lib_folder(lib_id), "%s_gene_expression_cpm.tab" % (lib_id))
        res, _ = pybio.utils.Cmd("grep -i '%s' %s" % (kw, fname)).run()
        res = res.split("\n")
        result = []
        for line in res:
            line = line.split("\t")
            if len(line)<2:
                continue
            result.append(line[0]+", " + line[1])
        result = self.sort_input_results(kw, result)
        return self.return_string(json.dumps({"keywords":result[:30]}))

    def get_ep2(self):
        lib_id = self.pars.get("lib_id", None)
        genes = self.pars.get("genes", None)
        initialize = self.pars.get("initialize", None)
        #genes = "DDB_G0267242,DDB_G0267248"
        if lib_id==None:
            return self.return_string("")
        fname = os.path.join(apa.path.lib_folder(lib_id), "salmon", "%s_salmon.tab" % (lib_id))
        result = []
        res = []
        if genes!=None:
            parsed_genes = []
            genes = genes.split("|||")
            for gene in genes:
                gene = gene.split(", ")
                gene = gene[0]
                parsed_genes.append(gene)
            parsed_genes = '|'.join(parsed_genes)
            res, _ = pybio.utils.Cmd("grep -iE '%s' %s" % (parsed_genes, fname)).run()
            res = res.split("\n")[:-1]
        if initialize=="yes":
            res, _ = pybio.utils.Cmd("head -n 6 %s" % (fname)).run()
            res = res.split("\n")[1:-1]
        # get header, we want values for experiments in a dictionary by experiment id
        header, _ = pybio.utils.Cmd("head -n 1 %s" % (fname)).run()
        header = header.split("\n")[0].split("\t")
        for line in res[:10]: # max results
            line = dict(zip(header, line.split("\t")))
            result.append(line)
        return self.return_string(json.dumps(result, default=dthandler))

    def get_keywords_genes2(self):
        lib_id = self.pars.get("lib_id", None)
        kw = self.pars.get("kw", "")
        if len(kw)<2:
            return self.return_string(json.dumps([]))
        if lib_id==None:
            return self.return_string("")
        fname = os.path.join(apa.path.lib_folder(lib_id), "salmon", "%s_salmon.tab" % (lib_id))
        res, _ = pybio.utils.Cmd("grep -i '%s' %s" % (kw, fname)).run()
        res = res.split("\n")
        result = []
        for line in res:
            line = line.split("\t")
            if len(line)<2:
                continue
            result.append(line[0]+", " + line[2])
        result = self.sort_input_results(kw, result)
        return self.return_string(json.dumps({"keywords":result[:30]}))

    def sort_input_results(self, inputString, unsortedResults):

        perfect_starting_matches = []
        perfect_matches = []
        partial_starting_matches = []
        rest_of_matches = []

        # First we need to have them sort them
        preliminary_sortedResults = sorted(unsortedResults)

        # remove the trailing "s"
        pSplit = re.compile(r"\s", re.UNICODE)
        unsortedResults_wo_s = []; i = 0

        for match in preliminary_sortedResults:
            words = []
            for word in re.split(pSplit, match):
                try:
                    if word[-1]=="s": word = word[:-1]
                except:
                    pass
                words.append(word)

            unsortedResults_wo_s.append( (" ".join(words), i) )
            i += 1

        sortedResults_wo_s = sorted(unsortedResults_wo_s)

        sortedResults = []
        for match, i in sortedResults_wo_s:
            sortedResults.append(preliminary_sortedResults[i])

        pPSM = re.compile(r"^%ss?(?![\w-])" % inputString, re.UNICODE|re.IGNORECASE)
        pPM  = re.compile(r"\b%ss?(?![\w-])" % inputString, re.UNICODE|re.IGNORECASE)
        pTSM  = re.compile(r"^%ss?" % inputString, re.UNICODE|re.IGNORECASE)

        for match in sortedResults:
            if re.search(pPSM, match):
                perfect_starting_matches.append(match)
            elif re.search(pPM, match):
                perfect_matches.append(match)
            elif re.search(pTSM, match):
                partial_starting_matches.append(match)
            else:
                rest_of_matches.append(match)
        return ( perfect_starting_matches+
                 perfect_matches+
                 partial_starting_matches+
                 rest_of_matches )

    # DELETE v1.1
    def get_comps(self):
        stats = {}

        def read_stats(lib_id):
            if stats.get(lib_id, None)==None:
                fname = os.path.join(apa.path.lib_folder(lib_id), "%s_m%s.stats.tab" % (lib_id, 1))
                res = {}
                f = open(fname, "rt")
                r = f.readline()
                r = f.readline()
                while r:
                    r = r.replace("\r", "").replace("\n", "").split("\t")
                    res[int(r[0])] = (r[-3], r[-1])
                    r = f.readline()
                f.close()
                stats[lib_id] = res

        def make_table(comps_data):
            res = []
            for (cid, exp_list, name) in comps_data:
                for exp_rec in exp_list:
                    lib_id = "_".join(exp_rec.split("_")[:-1])
                    exp_id = int(exp_rec.split("_")[-1][1:])
                    ann = copy.copy(apa.annotation.libs[lib_id].experiments[exp_id])
                    ann["lib_id"] = lib_id
                    ann["exp_id"] = exp_id
                    ann["cid"] = cid
                    read_stats(lib_id)
                    ann["stats"] = copy.copy(stats[lib_id][exp_id])
                    res.append(ann)
            return res

        comps_id = self.pars.get("comps_id", None)
        if comps_id==None:
            return self.return_string("empty")
        comps = apa.comps.Comps(comps_id)
        email = self.pars.get("email", "public")
        pair_type = self.pars.get("pair_type", "same")

        folder = os.path.join(apa.path.comps_folder, comps_id)
        last_change = os.stat(folder).st_mtime
        r = {}
        r["comps_id"] = comps_id
        r["comps_name"] = comps.name
        r["CLIP"] = comps.CLIP
        r["site_selection"] = comps.site_selection
        r["last_change"] = datetime.datetime.fromtimestamp(last_change)
        r["notes"] = comps.notes
        r["control"] = make_table(comps.control)
        r["test"] = make_table(comps.test)

        go = {} # read GO files
        for aspect in ["P", "C"]:
            for reg_type in ["enhanced", "repressed"]:
                fname = os.path.join(apa.path.comps_folder, comps_id, "rnamap", "go_%s_%s_%s.json" % (reg_type, pair_type, aspect))
                if os.path.exists(fname):
                    go["%s_%s_%s" % (reg_type, pair_type, aspect)] = json.loads(open(fname, "rt").readline())
                else:
                    go["%s_%s_%s" % (reg_type, pair_type, aspect)] = []
                for site_type in ["proximal", "distal"]:
                    fname = os.path.join(apa.path.comps_folder, comps_id, "rnamap", "go_%s_%s_%s_%s.json" % (reg_type, site_type, pair_type, aspect))
                    if os.path.exists(fname):
                        go["%s_%s_%s_%s" % (reg_type, site_type, pair_type, aspect)] = json.loads(open(fname, "rt").readline())
                    else:
                        go["%s_%s_%s_%s" % (reg_type, site_type, pair_type, aspect)] = []
        r["go"] = go
        return self.return_string(json.dumps(r, default=dthandler))

    def get_library_status(self):
        lib_id = self.pars.get("lib_id", None)
        if lib_id!=None:
            lib = apa.annotation.Library(lib_id)
            return self.return_string(lib.status)
        return self.return_string("")

    def get_library(self):

        apa.annotation.init()

        def read_stats(lib_id):
            result = {}
            fname = os.path.join(apa.path.lib_folder(lib_id), "%s_m%s.stats.tab" % (lib_id, 1))
            if os.path.exists(fname):
                f = open(fname, "rt")
                r = f.readline()
                r = f.readline()
                while r:
                    r = r.replace("\r", "").replace("\n", "").split("\t")
                    result[int(r[0])] = (r[-3], r[-1])
                    r = f.readline()
                f.close()
            return result

        library_id = self.pars.get("library_id", None)
        stats = read_stats(library_id)
        library = apa.annotation.libs.get(library_id, None)
        if library==None:
            return self.return_string("empty")
        email = self.pars.get("email", "public")
        library_folder = os.path.join(apa.path.data_folder, library_id)
        r = {}
        r["lib_id"] = library_id
        r["name"] = library.name
        r["notes"] = library.notes
        r["columns"] = [(e1, e2) for (e1, e2) in library.columns if e2 not in ["method", "map_to", "species", "method"]]
        r["columns_display"] = [(e1, e2) for (e1, e2) in library.columns_display if e2 not in ["method", "map_to", "species", "method"]]
        r["owner"] = library.owner
        r["access"] = library.access
        r["genome"] = library.genome
        r["tags"] = library.tags
        r["seq_type"] = library.seq_type
        r["genome_desc"] = db["genomes"][library.genome]["desc"]
        if r["genome_desc"]!="not selected":
            r["genome_desc"] = db["genomes"][library.genome]["desc"] % (db["genomes"][library.genome]["link_assembly"], db["genomes"][library.genome]["link_annotation"])
        r["method"] = library.method
        r["method_desc"] = db["methods"][library.method]["desc"]
        if r["method_desc"]!="not selected":
            r["method_desc"] = db["methods"][library.method]["desc"] % db["methods"][library.method]["link"]
        experiments = {}
        for exp_id, exp_data in library.experiments.items():
            exp_map_stats = stats.get(exp_id, ("", ""))
            exp_data["stats"] = [exp_map_stats[0], exp_map_stats[1]]
            exp_data["lib_id"] = library_id
            experiments[int(exp_id)] = exp_data
        r["experiments"] = experiments
        return self.return_string(json.dumps(r, default=dthandler))

    def save_library(self):
        apa.annotation.init()
        r = {"status":"fail"}
        lib_id = self.pars.get("lib_id", None)
        library = apa.annotation.libs.get(lib_id, None)
        email = self.pars.get("email", "public")
        if (email=="public"):
            return self.return_string(json.dumps(r, default=dthandler))
        if (email not in library.owner) and email!="gregor.rot@gmail.com":
            return self.return_string(json.dumps(r, default=dthandler))
        library.name = self.pars.get("name", "")
        library.notes = self.pars.get("notes", "")
        library.tags = self.pars.get("tags", "")
        library.method = self.pars.get("method", "")
        library.genome = self.pars.get("genome", "")
        library.access = self.pars.get("access", "").split(",")
        library.owner = self.pars.get("owner", "").split(",")
        library.columns = json.loads(self.pars["columns"])
        library.columns_display = json.loads(self.pars["columns_display"])
        library.experiments = json.loads(self.pars["experiments"])
        library.save()
        r = {"status":"success"}
        return self.return_string(json.dumps(r, default=dthandler))

    def save_analysis(self):
        apa.annotation.init()
        r = {"status":"fail"}
        analysis_id = self.pars.get("analysis_id", None)
        analysis = apa.comps.Comps(analysis_id)
        email = self.pars.get("email", "public")
        if (email=="public"):
            return self.return_string(json.dumps(r, default=dthandler))
        #if (email not in library.owner) and email!="gregor.rot@gmail.com":
        #    return json.dumps(r, default=dthandler)
        analysis.name = self.pars.get("name", "")
        analysis.notes = self.pars.get("notes", "")
        analysis.access = self.pars.get("access", "").split(",")
        analysis.owner = self.pars.get("owner", "").split(",")
        analysis.save()
        r = {"status":"success"}
        return self.return_string(json.dumps(r, default=dthandler))

    def new_library(self):
        def new_lib_id():
            data_folder = apa.path.data_folder
            prefix = "%s_" % (datetime.datetime.now().strftime("%Y%m%d"))

            libs = glob.glob(os.path.join(data_folder, "%s*" % prefix))
            if len(libs)==0:
                postfix = "1"
            else:
                postfix = 0
                for lib_id in libs:
                    postfix = max(postfix, int(lib_id.split(prefix)[1]), postfix)
                postfix += 1

            lib_id = "%s%s" % (prefix, postfix)
            return lib_id

        email = self.check_login(self.pars.get("email", "public"))
        genome = self.pars.get("genome", "")
        method = self.pars.get("method", "")
        seq_type = self.pars.get("seq_type", "")
        if email=="public":
            r = {"status":"fail"}
            return self.return_string(json.dumps(r, default=dthandler))
        lib_id = new_lib_id()
        lib_folder = os.path.join(apa.path.data_folder, lib_id)
        os.makedirs(lib_folder)
        library = apa.annotation.Library(lib_id)
        library.method = method
        library.genome = genome
        library.seq_type = seq_type
        library.owner = [email]
        library.access = [email]
        library.save()
        r = {"status":"success", "lib_id":lib_id}
        return self.return_string(json.dumps(r, default=dthandler))

    def delete_library(self):
        apa.annotation.init()
        email = self.check_login(self.pars.get("email", "public"))
        if email=="public":
            r = {"status":"fail"}
            return self.return_string(json.dumps(r, default=dthandler))
        lib_id = self.pars.get("library_id", None)
        if lib_id==None:
            r = {"status":"fail"}
            return self.return_string(json.dumps(r, default=dthandler))
        if len(lib_id)<=6:
            r = {"status":"fail"}
            return self.return_string(json.dumps(r, default=dthandler))
        library = apa.annotation.libs[lib_id]
        if (email not in library.owner):
            r = {"status":"fail"}
            return self.return_string(json.dumps(r, default=dthandler))
        lib_folder = os.path.join(apa.path.data_folder, lib_id)
        if lib_folder.startswith("/home/gregor/apa/data.apa/") and len(lib_folder)>(len("/home/gregor/apa/data.apa/")+6):
            if os.path.exists(lib_folder):
                shutil.rmtree(lib_folder)
        r = {"status":"success", "lib_id":lib_id}
        return self.return_string(json.dumps(r, default=dthandler))

    def delete_experiment(self):
        apa.annotation.init()
        email = self.check_login(self.pars.get("email", "public"))
        if email=="public":
            r = {"status":"fail"}
            return self.return_string(json.dumps(r, default=dthandler))
        lib_id = self.pars.get("lib_id", None)
        exp_id = self.pars.get("exp_id", None)
        if lib_id==None:
            r = {"status":"fail"}
            return self.return_string(json.dumps(r, default=dthandler))
        if len(lib_id)<=6 or exp_id==None:
            r = {"status":"fail"}
            return self.return_string(json.dumps(r, default=dthandler))
        library = apa.annotation.libs[lib_id]
        if (email not in library.owner):
            r = {"status":"fail"}
            return self.return_string(json.dumps(r, default=dthandler))
        lib_folder = os.path.join(apa.path.data_folder, lib_id)
        exp_folder = os.path.join(apa.path.data_folder, lib_id, "e"+exp_id)
        if lib_folder.startswith("/home/gregor/apa/data.apa/") and len(lib_folder)>(len("/home/gregor/apa/data.apa/")+6):
            if os.path.exists(exp_folder):
                shutil.rmtree(exp_folder)
        del library.experiments[int(exp_id)]
        library.save()
        r = {"status":"success", "lib_id":lib_id}
        return self.return_string(json.dumps(r, default=dthandler))

    def new_analysis(self):
        def new_analysis_id():
            data_folder = apa.path.comps_folder
            prefix = "%s_" % (datetime.datetime.now().strftime("%Y%m%d"))

            libs = glob.glob(os.path.join(data_folder, "%s*" % prefix))
            if len(libs)==0:
                postfix = "1"
            else:
                postfix = 0
                for lib_id in libs:
                    postfix = max(postfix, int(lib_id.split(prefix)[1]), postfix)
                postfix += 1

            analysis_id = "%s%s" % (prefix, postfix)
            return analysis_id

        email = self.check_login(self.pars.get("email", "public"))
        if email=="public":
            r = {"status":"fail"}
            return self.return_string(json.dumps(r, default=dthandler))
        analysis_name = self.pars.get("analysis_name", "")
        analysis_type = self.pars.get("analysis_type", "")
        analysis_genome = self.pars.get("genome", "")
        analysis_method = self.pars.get("method", "")
        experiments = self.pars.get("experiments", "")
        analysis_id = new_analysis_id()
        analysis_folder = os.path.join(apa.path.comps_folder, analysis_id)
        os.makedirs(analysis_folder)

        # write analysis config file
        f = open(os.path.join(analysis_folder, "%s.config" % analysis_id), "wt")
        f.write("id\texperiments\tname\n")
        experiments = json.loads(experiments)
        control_experiments = []
        test_experiments = []
        for exp_id, exp_data in experiments.items():
            if exp_data.get("analysis_set", None)=="groupA":
                control_experiments.append("%s_e%s" % (exp_data["lib_id"], exp_id))
            if exp_data.get("analysis_set", None)=="groupB":
                test_experiments.append("%s_e%s" % (exp_data["lib_id"], exp_id))

        control_experiments = sorted(control_experiments, key=lambda x: int(x.split("_")[2][1:])) # sort by exp_id
        test_experiments = sorted(test_experiments, key=lambda x: int(x.split("_")[2][1:])) # sort by exp_id

        for index, exp in enumerate(control_experiments):
            f.write("c%s\t%s\t%s\n" % ((index+1), exp, exp))
        for index, exp in enumerate(test_experiments):
            f.write("t%s\t%s\t%s\n" % ((index+1), exp, exp))

        f.write("\n")
        f.write("control_name:control\n")
        f.write("test_name:test\n")

        if analysis_type=="apa":
            f.write("site_selection:DEX\n")
            f.write("polya_db:20190523_1\n")
            f.write('poly_type:["strong", "weak", "less", "noclass"]\n')
            f.write("presence_thr:3\n")
            f.write("cDNA_thr:3\n\n")

        f.write("authors:%s\n" % email)
        f.write("access:%s\n" % email)
        f.write("owner:%s\n" % email)
        f.write("name:%s\n" % analysis_name)

        f.write("\n")
        f.write("analysis_type:%s\n" % analysis_type)
        f.write("method:%s\n" % analysis_method)
        f.write("genome:%s\n" % analysis_genome)
        f.write("\n")
        f.write("status:%s\n" % "processing")

        f.close()

        self.add_ticket(email, "apa.comps -comps_id %s" % (analysis_id), "process analysis %s" % (analysis_id))

        r = {"status":"success", "analysis_id":analysis_id}
        return self.return_string(json.dumps(r, default=dthandler))

    def delete_analysis(self):
        apa.annotation.init()
        email = self.check_login(self.pars.get("email", "public"))
        if email=="public":
            r = {"status":"fail", "message":"public email"}
            return self.return_string(json.dumps(r, default=dthandler))
        analysis_id = self.pars.get("analysis_id", None)
        if analysis_id==None:
            r = {"status":"fail", "message":"no analysis id"}
            return self.return_string(json.dumps(r, default=dthandler))
        if len(analysis_id)==0:
            r = {"status":"fail", "message":"id too short"}
            return self.return_string(json.dumps(r, default=dthandler))
        analysis = apa.comps.Comps(analysis_id)
        if (email not in analysis.access):
            r = {"status":"fail", "message":"no access with email"}
            return self.return_string(json.dumps(r, default=dthandler))
        analysis_folder = os.path.join(apa.path.comps_folder, analysis_id)
        if analysis_folder.startswith("/home/gregor/apa/data.comps/") and len(analysis_folder)>(len("/home/gregor/apa/data.comps/")+1):
            if os.path.exists(analysis_folder):
                shutil.rmtree(analysis_folder)
        r = {"status":"success", "analysis_id":analysis_id}
        apa.annotation.init()
        return self.return_string(json.dumps(r, default=dthandler))

    def rnamap(self):
        clip_index = self.pars.get("clip_index", 0)
        comps_id = self.pars.get("comps_id", None)
        pair_type = self.pars.get("pair_type", "same")
        site_type = self.pars.get("site_type", "proximal")
        fname = os.path.join(apa.path.comps_folder, comps_id, "rnamap", "clip%s.%s.%s.tab" % (clip_index, pair_type, site_type))
        if os.path.exists(fname):
            return self.return_string(open(fname).readline())
        else:
            return self.return_string(json.dumps({"status":"no results"}))

    def rnaheat(self):
        comps_id = self.pars.get("comps_id", None)
        reg = self.pars.get("reg", "pos")
        clip_index = self.pars.get("clip_index", "0")
        pair_type = self.pars.get("pair_type", "samen")
        site_type = self.pars.get("site_type", "proximal")
        fname = os.path.join(apa.path.comps_folder, comps_id, "rnamap", "clip%s_heat.%s.%s_%s_json.tab" % (clip_index, pair_type, site_type, reg))
        if os.path.exists(fname):
            return self.return_string(open(fname).readline())
        else:
            return self.return_string(json.dumps({"status":"no results"}))

    def apamap(self):
        analysis_id = self.pars.get("analysis_id", None)
        pair_type = self.pars.get("pair_type", "same")
        pairs_filename = os.path.join(apa.path.comps_folder, analysis_id, "%s.pairs_de.tab" % analysis_id)
        plot_data = {"enhanced" : {"x":[], "y":[], "gene_id":[]}, "repressed" : {"x":[], "y":[], "gene_id":[]}, "control_up" : {"x":[], "y":[], "gene_id":[]}, "control_down" : {"x":[], "y":[], "gene_id":[]}}
        f = open(pairs_filename, "rt")
        header = f.readline().replace("\r", "").replace("\n", "").split("\t")
        r = f.readline()
        while r:
            r = r.replace("\r", "").replace("\n", "").split("\t")
            data = dict(zip(header, r))
            if data["pair_type"]==pair_type or pair_type=="combined":
                plot_data[data["gene_class"]]["x"].append(float(data["proximal_fc"]))
                plot_data[data["gene_class"]]["y"].append(float(data["distal_fc"]))
                plot_data[data["gene_class"]]["gene_id"].append("%s: %s" % (data["gene_id"], data["gene_name"]))
            r = f.readline()
        f.close()
        return self.return_string(json.dumps(plot_data))

    def check_login(self, email="public"):
        conn = Session()
        q = conn.query(Users).filter(and_(Users.email==email)).all()
        if len(q)==1:
            return q[0].email
        else:
            return "public"

    def login(self, email="public"):
        email = self.pars.get("email", email)
        conn = Session()
        q = conn.query(Users).filter(Users.email==email).all()
        result = {}
        if len(q)==0:
            u = Users()
            u.email = email
            u.last_login = datetime.datetime.now()
            conn.add(u)
            conn.commit()
            result["news"] = 1
            result["email"] = email
            result["status"] = "ok"
            result["usertype"] = "guest"
            result["tickets"] = "[]";
            result["libs"], result["experiments"] = 0, 0
            if email!="gregor.rot@gmail.com":
                self.send_email("gregor.rot@gmail.com", "expressRNA: user login", "Dear Gregor,\n\n%s is a new user with expressRNA!,\n\nBest,\nexpressRNA" % email)
        if len(q)==1:
            q[0].last_login = datetime.datetime.now()
            conn.commit()
            result["news"] = q[0].news
            result["email"] = q[0].email
            result["status"] = "ok"
            result["usertype"] = q[0].usertype
            result["libs"], result["experiments"] = self.count_ownership(q[0].email)
            result["tickets"] = self.get_tickets(email)
        return self.return_string(json.dumps(result, default=dthandler))

    def update_user_usage(self):
        result = {}
        email = self.pars.get("email", None)
        if email==None:
            return
        result["libs"], result["experiments"] = self.count_ownership(email)
        return self.return_string(json.dumps(result, default=dthandler))

    def get_tickets(self, email):
        tickets = []
        conn = Session()
        if email=="gregor.rot@gmail.com":
            q = conn.query(Tickets).filter(Tickets.date_finished==None).order_by(Tickets.tid).all()
        else:
            q = conn.query(Tickets).filter(Tickets.date_finished==None).filter(Tickets.email==email).order_by(Tickets.tid).all()
        for rec in q:
            if rec.date_started!=None:
                try:
                    processing_time = round((datetime.datetime.now() - rec.date_started).total_seconds() / 60.0)
                except:
                    processing_time = "0"
            else:
                processing_time = ""
            row = {"processing_time":processing_time, "tid":rec.tid, "date_added":rec.date_added, "date_started":rec.date_started, "date_finished":rec.date_finished, "desc":rec.desc, "status":rec.status, "minutes":rec.minutes}
            tickets.append(row)
        return tickets

    def refetch_tickets(self):
        email = self.pars.get("email", None)
        if email==None:
            return self.return_string("refetch_tickets fail")
        return self.return_string(json.dumps(self.get_tickets(email), default=dthandler))

    def purchase_support(self):
        email = self.pars.get("email", None)
        product = self.pars.get("product", None)
        if email==None or product==None:
            return self.return_string("purchase_support fail")
        stripe_session = stripe.checkout.Session.create(
          payment_method_types=['card'],
          customer_email=email,
          line_items=[{
            'name': '1h bioinformatics support',
            'description': '1h for bioinformatics analysis of you datasets',
            'amount': 5000,
            'currency': 'chf',
            'quantity': 1,
          }],
          success_url='https://expressRNA.org/1hsuccess?session_id={CHECKOUT_SESSION_ID}',
          cancel_url='https://expressRNA.org/1hcancel',
        )
        return self.return_string(json.dumps(stripe_session, default=dthandler))

    # todo: parameter once=True/False
    def add_ticket(self, email, command, desc, once=True):
        conn = Session()
        if once: # remove all previous tickets with status = 0
            conn.execute("delete from tickets where status=0 and command='%s'" % (command))
            conn.commit()
        t = Tickets()
        t.email = email
        t.command = command
        t.desc = desc
        t.status = 0
        t.date_added = datetime.datetime.now()
        conn.add(t)
        conn.commit()
        conn.flush()

    def save_login(self):
        data = self.pars.get("data", None)
        if data==None:
            return self.return_string("fail")
        data = json.loads(data)
        conn = Session()
        q = conn.query(Users).filter(Users.email==data["email"]).all()
        if len(q)==1:
            q[0].last_login = datetime.datetime.now()
            q[0].news = data["news"]
            conn.commit()
        return self.return_string("saved")

    def count_ownership(self, email):
        apa.annotation.init()
        num_libs = 0
        num_experiments = 0
        for exp_id, data in apa.annotation.libs.items():
            if email in data.owner:
                num_libs += 1
                num_experiments += len(data.experiments)
        return num_libs, num_experiments

    def get_server_stats(self):
        result = {}
        result["cpu"] = psutil.cpu_percent(percpu=True)
        return self.return_string(json.dumps(result, default=dthandler))

    def get_server_data_stats(self):
        result = {}
        result["data"] = json.load(open(os.path.join(apa.path.data_folder, "stats.json")))
        return self.return_string(json.dumps(result, default=dthandler))
"""


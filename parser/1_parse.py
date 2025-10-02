from lxml import etree
import glob
from tqdm import tqdm
from unidecode import unidecode
import re
import gzip

def remove_special_characters(text):
    t1 = unidecode(text)
    return re.sub(r'[\x00-\x1F\x7F\u2028\u2029]', '', t1).replace("\\", "")

authors_pmids = {}
pmids = set()
fout_pub = gzip.open("publications.tab.gz", "wt")
xml_files = glob.glob("../database/*.xml.gz")
nfiles = len(xml_files)
c = 0
npub = 0
for xml_file in xml_files:
    c += 1
    tree = etree.parse(xml_file)
    for article in tree.xpath("//PubmedArticle"):
        npub += 1
        # PMID
        pmid_node = article.find(".//PMID")
        if pmid_node is None:
            continue
        pmid = int(pmid_node.text)

        title_node = article.find(".//ArticleTitle")
        if title_node is not None:
            title_node = "".join(title_node.itertext()).strip() # because .text returns None for cases of titles containing tags (<>)
        title = title_node if title_node is not None else ""
        if title=="":
            continue
        title = remove_special_characters(title)

        # Journal
        journal_node = article.find(".//Journal/Title")
        journal = journal_node.text if journal_node is not None else ""
        if journal==None: # even from Xml, can be returned as None
            journal = ""
        journal = remove_special_characters(journal)

        # Year
        year_node = article.find(".//PubDate/Year")
        year = int(year_node.text) if year_node is not None and year_node.text.isdigit() else ""
        if year==None: # even from Xml, can be returned as None
            year = ""
        if year=="":
            continue

        publication = []
        publication.append(pmid)
        publication.append(title)
        publication.append(year)
        author_rec = []
        for author in article.xpath(".//AuthorList/Author"):
            lastname = author.findtext("LastName")
            forename = author.findtext("ForeName")
            initials = author.findtext("Initials")
            affil = author.findtext("AffiliationInfo/Affiliation")
            author_text = f"{forename} {lastname}"
            author_rec.append(author_text)
        authors_list = []
        for rec in author_rec:
            author_name = remove_special_characters(rec).lower()
            if author_name == "none none":
                continue
            authors_list.append(author_name)
            authors_pmids.setdefault(author_name, set()).add(pmid)
        publication.append(",".join(authors_list))

        # 20250930: adding mesh terms to publications
        #mesh_terms = []
        #for mesh in article.xpath(".//MeshHeadingList/MeshHeading/DescriptorName"):
        #    if mesh is not None and mesh.text:
        #        mesh_terms.append(remove_special_characters(mesh.text).lower())
        #publication.append("|".join(mesh_terms))

        if pmid not in pmids: # there are some duplicate (3165) pmids 
            fout_pub.write("\t".join([str(x) for x in publication]) + "\n")
            pmids.add(pmid)
        if npub%10000==0:
            print(xml_file, f"{c}/{nfiles}, {npub/1000000}M publications parsed")

fout_pub.close()

fout_aut = gzip.open("authors.tab.gz", "wt")
for author_name, pmids in authors_pmids.items():
    fout_aut.write(f"{author_name}\t{','.join([str(x) for x in pmids])}\n")
fout_aut.close()

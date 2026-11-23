"""Bounded classic-xref PDF dependency inspection, never a PDF renderer.

Only structural objects are parsed. Stream bytes are skipped by their resolved
Length; literal/hex strings and comments never become dictionary names. Modern
xref/object streams and encryption remain unsupported and HOLD cleanup.
"""
import re
import math
from collections import namedtuple


class PdfHeld(ValueError):
    pass


class Name(str):
    pass


Ref = namedtuple('Ref', 'number generation')
SPACE = b'\0\t\n\f\r '
DELIMS = SPACE + b'()<>[]{}/%'


def need(ok, code='unsupported_pdf_structure'):
    if not ok:raise PdfHeld(code)


class Inspector:
    def __init__(self, raw):
        self.raw=raw;self.index={};self.objects={};self.loading=set();self.tokens=0

    def space(self,p):
        while p<len(self.raw):
            if self.raw[p] in SPACE:p+=1
            elif self.raw[p]==37:
                while p<len(self.raw) and self.raw[p] not in b'\r\n':p+=1
            else:break
        return p

    def token(self,p):
        p=self.space(p);self.tokens+=1
        need(self.tokens<=2000000,'pdf_structure_budget_exceeded')
        need(p<len(self.raw))
        for marker in (b'<<',b'>>'):
            if self.raw.startswith(marker,p):return marker,p+2
        c=self.raw[p]
        if c in b'[]':return bytes([c]),p+1
        if c==47:
            end=p+1
            while end<len(self.raw) and self.raw[end] not in DELIMS:end+=1
            name=self.raw[p+1:end]
            need(not re.search(rb'#(?![0-9a-fA-F]{2})',name))
            name=re.sub(rb'#([0-9a-fA-F]{2})',lambda m:bytes([int(m[1],16)]),name)
            return Name(name.decode('latin1')),end
        if c==40:
            depth=1;p+=1
            while p<len(self.raw) and depth:
                c=self.raw[p];p+=1
                if c==92:
                    need(p<len(self.raw));p+=1
                    if self.raw[p-1]==13 and p<len(self.raw) and self.raw[p]==10:p+=1
                elif c==40:depth+=1
                elif c==41:depth-=1
                need(depth<=64,'pdf_structure_budget_exceeded')
            need(depth==0);return ('string',),p
        if c==60:
            end=self.raw.find(b'>',p+1);need(end>=0)
            need(all(x in SPACE+b'0123456789abcdefABCDEF' for x in self.raw[p+1:end]))
            return ('string',),end+1
        end=p
        while end<len(self.raw) and self.raw[end] not in DELIMS:end+=1
        need(end>p)
        return self.raw[p:end],end

    def value(self,p,depth=0):
        need(depth<=64,'pdf_structure_budget_exceeded')
        token,end=self.token(p)
        if isinstance(token,Name) or isinstance(token,tuple):return token,end
        if token==b'<<':
            value={}
            while True:
                key,nextp=self.token(end)
                if key==b'>>':return value,nextp
                need(isinstance(key,Name) and key not in value)
                value[key],end=self.value(nextp,depth+1)
        if token==b'[':
            value=[]
            while True:
                child,nextp=self.token(end)
                if child==b']':return value,nextp
                child,end=self.value(end,depth+1);value.append(child)
        if token in (b'true',b'false',b'null'):return {b'true':True,b'false':False,b'null':None}[token],end
        need(re.fullmatch(rb'[+-]?(?:\d+(?:\.\d*)?|\.\d+)',token) is not None)
        number=float(token) if b'.' in token else int(token)
        # References require two unsigned integer tokens followed by R.
        if type(number) is int and number>=0:
            following,after=self.token(end)
            if type(following) is bytes and re.fullmatch(rb'\d+',following):
                marker,last=self.token(after)
                if marker==b'R':return Ref(number,int(following)),last
        return number,end

    def xrefs(self,offset,seen=None):
        seen=set() if seen is None else seen
        need(offset not in seen and len(seen)<32,'pdf_xref_cycle_or_budget');seen.add(offset)
        token,p=self.token(offset);need(token==b'xref','unsupported_pdf_xref_stream')
        local={}
        while True:
            token,p=self.token(p)
            if token==b'trailer':break
            count,p=self.token(p)
            need(type(token) is bytes and re.fullmatch(rb'\d+',token)
                 and type(count) is bytes and re.fullmatch(rb'\d+',count))
            start,count=int(token),int(count);need(count<=200000,'pdf_structure_budget_exceeded')
            for number in range(start,start+count):
                at,p=self.token(p);generation,p=self.token(p);state,p=self.token(p)
                need(re.fullmatch(rb'\d{10}',at) and re.fullmatch(rb'\d{5}',generation) and state in (b'n',b'f'))
                need(number not in local)
                local[number]=(int(generation),int(at)) if state==b'n' else None
        trailer,p=self.value(p);need(isinstance(trailer,dict))
        need('Encrypt' not in trailer and 'XRefStm' not in trailer,'unsupported_pdf_encryption_or_hybrid_xref')
        # Newer xref sections (including frees) override prior revisions.
        for number,entry in local.items():self.index.setdefault(number,entry)
        if 'Prev' in trailer:
            need(type(trailer['Prev']) is int and 0<=trailer['Prev']<len(self.raw))
            self.xrefs(trailer['Prev'],seen)

    def object(self,ref):
        if ref in self.objects:return self.objects[ref]
        need(ref not in self.loading,'pdf_indirect_cycle')
        entry=self.index.get(ref.number);need(entry is not None and entry[0]==ref.generation,'pdf_unresolved_reference')
        self.loading.add(ref)
        try:
            number,p=self.token(entry[1]);generation,p=self.token(p);marker,p=self.token(p)
            need(number==str(ref.number).encode() and generation==str(ref.generation).encode() and marker==b'obj')
            value,p=self.value(p);marker,end=self.token(p)
            if marker==b'stream':
                need(isinstance(value,dict) and 'Length' in value)
                length=self.resolve(value['Length']);need(type(length) is int and length>=0)
                if self.raw.startswith(b'\r\n',end):end+=2
                elif self.raw[end:end+1] in (b'\n',b'\r'):end+=1
                else:need(False)
                need(end+length<=len(self.raw))
                marker,p=self.token(end+length);need(marker==b'endstream')
                marker,end=self.token(p)
            need(marker==b'endobj')
            self.objects[ref]=value;return value
        finally:self.loading.remove(ref)

    def resolve(self,value):
        seen=set()
        while isinstance(value,Ref):
            need(value not in seen and len(seen)<64,'pdf_indirect_cycle');seen.add(value)
            value=self.object(value)
        return value

    def local_destination(self,value):
        value=self.resolve(value)
        need(isinstance(value,list) and len(value)>=2 and isinstance(value[0],Ref),
             'unsupported_pdf_open_action')
        page=self.resolve(value[0])
        need(isinstance(page,dict) and self.resolve(page.get('Type'))==Name('Page'),
             'unsupported_pdf_open_action')
        modes={'XYZ':3,'Fit':0,'FitH':1,'FitV':1,'FitR':4,'FitB':0,'FitBH':1,'FitBV':1}
        mode=value[1]
        need(isinstance(mode,Name) and mode in modes and len(value)==2+modes[mode],
             'unsupported_pdf_open_action')
        for operand in value[2:]:
            operand=self.resolve(operand)
            need((operand is None and mode!='FitR') or
                 (type(operand) in (int,float) and math.isfinite(operand)),
                 'unsupported_pdf_open_action')

    def inspect(self,value,depth=0):
        need(depth<=64,'pdf_structure_budget_exceeded')
        if isinstance(value,Ref):
            self.object(value)  # Every referenced object must actually resolve.
        elif isinstance(value,dict):
            need(not set(value)&{'FFilter','FDecodeParms','Reference','Ref','Alternates','OPI',
                 'URI','FS','UF','DOS','Mac','Unix','EF','RF','JS','JavaScript','AA'},
                 'unsupported_pdf_external_resource')
            if 'OpenAction' in value:
                need(self.resolve(value.get('Type'))==Name('Catalog'),'unsupported_pdf_open_action')
                self.local_destination(value['OpenAction'])
            need(self.resolve(value.get('Type')) not in (Name('Filespec'),Name('ObjStm'),Name('XRef')),
                 'unsupported_pdf_external_resource')
            need(self.resolve(value.get('S')) not in (Name('URI'),Name('GoToR'),Name('GoToE'),Name('Launch'),Name('JavaScript')),
                 'unsupported_pdf_external_resource')
            if 'F' in value:
                flags=self.resolve(value['F'])
                need(self.resolve(value.get('Type'))==Name('Annot') and type(flags) is int and 0<=flags<=0xffffffff,
                     'unsupported_pdf_external_resource')
            for child in value.values():self.inspect(child,depth+1)
        elif isinstance(value,list):
            for child in value:self.inspect(child,depth+1)

    def run(self):
        need(self.raw.startswith(b'%PDF-') and b'%%EOF' in self.raw[-1024:],'invalid_pdf_header')
        endings=list(re.finditer(rb'startxref\s+(\d+)\s+%%EOF',self.raw[-2048:]));need(endings)
        offset=int(endings[-1][1]);need(0<=offset<len(self.raw));self.xrefs(offset)
        for number,entry in self.index.items():
            if entry is not None:self.inspect(self.object(Ref(number,entry[0])))


def inspect_pdf(raw):
    Inspector(raw).run()

from .column import MetadataColumn
from .metadata import Metadata

def metadata_parse(s):
    md = Metadata()
    reorder = False
    lines = s.splitlines()
    for line in lines:
        cstart = line.find('#')
        if cstart != -1:
            line = line[:cstart]
        line = line.strip()
        if line == "":
            continue
        items = line.split(' ')
        if items[0] == 'column':
            name = items[1]
            type_ = items[2]
            meaning = None
            compression = None
            if len(items) > 3:
                meaning = items[3]
                if meaning == '_':
                    meaning = None
            if len(items) > 4:
                compression = items[4]
            md.add_column(MetadataColumn(name, type_, meaning, compression))
        elif items[0] == 'checksum':
            checksum_type = items[1]
            md.set_checksum(checksum_type)
        elif items[0] == 'reorder':
            reorder = True
        else:
            raise Exception('unknown line {}'.format(line))
    md.finalize(reorder)
    return md

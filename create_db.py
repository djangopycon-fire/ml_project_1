import sqlite3
import os
import xml.etree.cElementTree as etree
import logging

ques_dict= {
    'posts': {
        'Id': 'INTEGER',
        'PostTypeId': 'INTEGER', 
        'ParentID': 'INTEGER',
        'AcceptedAnswerId': 'INTEGER',  
        'CreationDate': 'DATETIME',
        'Score': 'INTEGER',
        'ViewCount': 'INTEGER',
        'Body': 'TEXT',
        'OwnerUserId': 'INTEGER', 
        'OwnerDisplayName': 'TEXT',
        'LastEditorUserId': 'INTEGER',
        'LastEditorDisplayName': 'TEXT', 
        'LastEditDate': 'DATETIME',  
        'LastActivityDate': 'DATETIME',  
        'CommunityOwnedDate': 'DATETIME',  
        'Title': 'TEXT',
        'Tags': 'TEXT',
        'AnswerCount': 'INTEGER',
        'CommentCount': 'INTEGER',
        'FavoriteCount': 'INTEGER',
        'ClosedDate': 'DATETIME'
    },
    'tags': {
        'Id': 'INTEGER',
        'TagName': 'TEXT',
        'Count': 'INTEGER',
        'ExcerptPostId': 'INTEGER',
        'WikiPostId': 'INTEGER'
    }
}


def dump_files(file_names, ques_dict,
               dump_path='.',
               dump_database_name='ritu.db',
               create_query='CREATE TABLE IF NOT EXISTS {table} ({fields})',
               insert_query='INSERT INTO {table} ({columns}) VALUES ({values})',
               log_filename='ritu.log'):
    
    db = sqlite3.connect(os.path.join(dump_path, dump_database_name))
    for file in file_names:
        with open(os.path.join(dump_path, file + '.xml')) as xml_file:
            tree = etree.iterparse(xml_file)
            table_name = file

            sql_create = create_query.format(table=table_name,
                  fields=", ".join(['{0} {1}'.format(name, type) for name, type in        ques_dict[table_name].items()]))
            print('Table {0} has been successfully created'.format(table_name))
            
            try:
                logging.info(sql_create)
                db.execute(sql_create)
            except Exception, e:
                logging.warning(e)

            for events, row in tree:
                try:
                    if row.attrib.values():
                        logging.debug(row.attrib.keys())
                        query = insert_query.format(
                            table=table_name,
                            columns=' ,'.join(row.attrib.keys()),
                            values=('?, ' * len(row.attrib.keys()))[:-2])
                        db.execute(query, row.attrib.values())
                        print ".",
                except Exception, e:
                    logging.warning(e)
                    print "x",
                finally:
                    row.clear()

            print "\n"
            # saving into dtatabase
            db.commit()
            del (tree)


if __name__ == '__main__':
    dump_files(ques_dict.keys(), ques_dict)

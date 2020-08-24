import os
import fileinput


MODELS_PATH = ''


def set_reserved_patterns():
    '''
    Using a list of defined reserved SQL keywords, creates a list of the
    possible different ways that keyword could appear in a query based on
    spacing and newlines e.g. "/nSUM", " SUM", etc.
    '''
    reserved_patterns = []

    reserved_words = [
    'abs('
    ,'alter'
    ,'and'
    ,'as'
    ,'avg('
    ,'between'
    ,'bigint'
    ,'bool_and('
    ,'bool_or('
    ,'boolean'
    ,'by'
    # the below is dumb and should be updated in the logic
    ,'SUM(case'
    ,'case'
    ,'coalesce('
    ,'concat('
    ,'contains('
    ,'convert_timezone('
    ,'count('
    ,'current_date('
    ,'current_time('
    ,'current_timestamp('
    ,'date('
    ,'datediff('
    ,'date_part('
    ,'date_trunc('
    ,'day('
    ,'decimal('
    ,'dense_rank('
    ,'desc'
    ,'distinct'
    ,'else'
    ,'end'
    ,'end)'
    ,'false'
    ,'first('
    ,'first_value('
    ,'float'
    ,'greatest('
    ,'group'
    ,'having'
    ,'hour('
    ,'ignore'
    ,'ilike'
    ,'in'
    ,'inner'
    ,'int'
    ,'integer'
    ,'isnull('
    ,'join'
    ,'last_value('
    ,'leading('
    ,'least('
    ,'left'
    ,'len('
    ,'length('
    ,'like'
    ,'limit'
    ,'max('
    ,'min('
    ,'month('
    ,'not'
    ,'null'
    ,'nullable'
    ,'nullif('
    ,'nulls'
    ,'offset('
    ,'on'
    ,'or'
    ,'order'
    ,'outer'
    ,'over'
    ,'partition'
    ,'preceding'
    ,'real'
    ,'regexp_replace('
    ,'reg_substr('
    ,'rows'
    ,'row_number('
    ,'second('
    ,'split_part('
    ,'sum('
    ,'then'
    ,'time('
    ,'timestamp('
    ,'to_char('
    ,'true'
    ,'truncate('
    ,'unbounded'
    ,'union'
    ,'upper('
    ,'varchar'
    ,'when'
    ,'where'
    ,'window'
    ,'with'
    ,'year('
    ]

    for reserved_word in reserved_words:
        if "(" in reserved_word:
            reserved_patterns.append(" "+reserved_word)
        else:
            # note I think this will break for a lot of variables e.g. AS is_red
            reserved_patterns.append(reserved_word+"\n")
            reserved_patterns.append(" "+reserved_word+" ")
            reserved_patterns.append(" "+reserved_word+"\n")

    return reserved_patterns


def get_code_block_status(line, current_status):
    '''
    given a line of SQL, determine if the line of SQL is either a comment
    or part of a comment block
    '''


    if "*/" in line:
        return "end of code block"
    elif "/*" in line or current_status == "in code block":
        return "in code block"
    elif current_status == "end of code block":
        return "not in code block"
    else:
        return "not in code block"


def format_reserved_words(reserved_patterns):
    '''
    iterate through every sql file in your models directory and determine is any
    reserved keywords should be capitalized
    '''

    for root, dirs, files in os.walk(MODELS_PATH):
        for name in files:

            if name.endswith(".sql"):
                lines = []
                with open(os.path.join(root, name),'r') as fr:
                    current_block_status = ""
                    for line in fr:
                        print(repr(line))
                        current_block_status = get_code_block_status(line, current_block_status)
                        if "--" not in line and current_block_status == "not in code block":
                            for pattern in reserved_patterns:
                                if " " in pattern:
                                    line = line.replace(pattern, pattern.upper())
                                elif line.replace(pattern, pattern.upper()) == 0:
                                    line = line.replace(pattern, pattern.upper())

                        lines.append(line)

                with open(os.path.join(root, name),'w') as fw:
                    for line in lines:
                        fw.write(line)


def run():
    reserved_patterns = set_reserved_patterns()
    format_reserved_words(reserved_patterns)

run()

// file: csv_parser.c
//

#include "csv_parser.h"
#include <ctype.h>   // for isspace

// private:
struct csv_parser_data
{
	CSV_CB_record_handler callback;
	void				* params;
	char				* buff;
	int				   field_count;
	const char *		  column_values[MAX_COLUMN_COUNT];
};

/* digest form CSV wiki http://en.wikipedia.org/wiki/Comma-separated_values

	* fields that contain commas, double-quotes, or line-breaks must be quoted,
	* a quote within a field must be escaped with an additional quote
	  immediately preceding the literal quote,
	* space before and after delimiter commas may be trimmed (which is
	  prohibited by RFC 4180), and
	* a line break within an element must be preserved.
*/



// returns 0 on success, E_QUOTED_STRING ON improperly quoted string
//
// pre-condition: **buff points to the beginning quote
// post-condition: **buff points to just before either a comma,
//			  or a newline, or E_QUOTED_STRING is returned.
//
static int csv_process_quoted_string(char **buff)
{
	char * q = *buff;
	char * p = q++;
	while (1)
	{
		switch(*q)
		{
		case '\0': // end of line in Quoted String, it's an error
			return E_QUOTED_STRING;
		case '"': // if the next char is not a '"', the QuotedString ends.
			if(*++q!='"')
				goto done_quoted_string;
			// here we deliberately let the else case fall through to default
			// processing
			//
		default:
			*p=*q;
			break;
		}
		++p, ++q;
	}
done_quoted_string:
	*p='\0';

	while( *q==' ' || *q=='\t' )
		++q;
	if( *q!=',' && *q!='\n' && *q!='\0')
		return E_QUOTED_STRING;
	*buff=--q;
	return 0;

}

//  returns
//   >= 0 : to continue parse next record
//   <  0 :  abort processing
//	   E_QUOTED_STRING is a special case of non-zero return values
//
static int csv_parse_line (struct csv_parser_data * d)
{
	char c;
	char * buff=d->buff;
	d->column_values[0]= buff;
	d->field_count=1;

	while ( (c=*buff)!='\n' )
	{
        if (c == '"') {
            // beginning a quoted string
			if( E_QUOTED_STRING==csv_process_quoted_string(&buff) )
				return E_QUOTED_STRING;
		} else if (c == ',') {
            // mark the end of the current field, and beginning of next field
			*buff='\0';
			d->column_values[d->field_count++]=buff+1;
        }
		++buff;
	}
	// now buff points to '\n', replace it with a '\0'
	*buff='\0';

	d->callback (d->params, d->field_count, d->column_values);

    return (buff - d->buff + 1);
}
// returns
//  0: on success
//  E_LINE_TOO_WIDE: on line too wide
//  E_QUOTED_STRING: at least 1 Quoted String is ill-formatted
//
int csv_parse(FILE *fp, CSV_CB_record_handler cb, void *params)
{
	char buff[MAX_LINE_LEN];
	struct csv_parser_data d;
	
	d.callback = cb;
	d.params   = params;
    d.buff = buff;

	while ( d.buff[MAX_LINE_LEN-1]='*',
			NULL!= fgets_unlocked (d.buff, MAX_LINE_LEN, fp)
	){
		int r;
		if(d.buff[MAX_LINE_LEN-1]=='\0' && d.buff[MAX_LINE_LEN-2]!='\n')
			return E_LINE_TOO_WIDE;
		if (E_QUOTED_STRING==(r=csv_parse_line (&d) ) )
			return E_QUOTED_STRING;
		else if (r<0)
			break;
	}
	return 0;
}

int csv_parse_eof(FILE *fp, CSV_CB_record_handler cb, void *params)
{
	char buff[MAX_LINE_LEN];
	struct csv_parser_data d;
	
	d.callback = cb;
	d.params   = params;
    d.buff = buff;

	while ( d.buff[MAX_LINE_LEN-1]='*',
			NULL!= fgets_unlocked (d.buff, MAX_LINE_LEN, fp) ) {
		int r;


		if(d.buff[MAX_LINE_LEN-1]=='\0' && d.buff[MAX_LINE_LEN-2]!='\n')
			return E_LINE_TOO_WIDE;
		if (E_QUOTED_STRING==(r=csv_parse_line (&d) ) )
			return E_QUOTED_STRING;
		else if (r<0)
			break;
	}
	return 0;
}

int csv_parse_fast(int fd, int bsize, CSV_CB_record_handler cb, void *params, int linemax)
{
	char buff[bsize];
	struct csv_parser_data d;
    void *p, *b;
    int br, bl, bt, last_line = 0;
	
	d.callback = cb;
	d.params   = params;

    p = b = buff;
    bt = bsize;

    /* read in some data */
readin:
    bl = 0;
    do {
        br = read(fd, p + bl, bt - bl);
        if (br < 0) {
            perror("read");
            goto out;
        } else if (br == 0) {
            /* read EOF */
            last_line = 1;
            break;
        }
        bl += br;
    } while (bl < bt);
    p = b;

    /* handle to parse line */
    while (bsize - (p - b) > linemax || last_line) {
        int r;

        d.buff = p;
		if (E_QUOTED_STRING == (r = csv_parse_line(&d))) {
			return E_QUOTED_STRING;
        } else if (r < 0)
			break;
        p += r;
        if (p - b >= bl)
            break;
    }

    /* we should memmove the remain buffer */
    bl = (bsize - (p - b));
    memmove(b, p, bl);
    p = b + bl;
    bt = bsize - bl;
    if (!last_line)
        goto readin;

out:
	return 0;
}

// file: csv_parser.h

#ifndef _CSV_PARSER_H_
#define _CSV_PARSER_H_

#include <stdio.h>  // for fopen, fclose, etc.
#include <string.h> // for strlen
#include <errno.h>
#include <unistd.h>


#define MAX_LINE_LEN (128 * 1024)
#define MAX_COLUMN_COUNT 1024
/* digest from CSV wiki: http://en.wikipedia.org/wiki/Comma-separated_values

	* fields that contain commas, double-quotes, or line-breaks must be quoted,
	* a quote within a field must be escaped with an additional quote
	  immediately preceding the literal quote,
	* space before and after delimiter commas may be trimmed (which is
	  prohibited by RFC 4180), and
	* a line break within an element must be preserved.
*/

enum { E_LINE_TOO_WIDE=-2, // error code for line width >= MAX_LINE_LEN
	   E_QUOTED_STRING=-3,	 // error code for ill-formatted quoted string
       E_PARTITAL_LINE=-4,
};

// mimic sqlite callback interface
//
typedef int (*CSV_CB_record_handler)
(
	void * params,
	int colum_cnt,
	const char ** column_values
);

int csv_parse (FILE *fp, CSV_CB_record_handler cb, void *params);
int csv_parse_eof (FILE *fp, CSV_CB_record_handler cb, void *params);
int csv_parse_fast(int fd, CSV_CB_record_handler cb, void *params, int linemax);

#endif

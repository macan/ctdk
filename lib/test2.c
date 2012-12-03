//---------------------------------------------------------------------------
// file: test1.c
//

#include "csv_parser.h"
#include <stdio.h>  // for fopen, fclose, etc.
#include <stdlib.h> // for atof;

struct process_sales_data
{
	float min;
	float max;
	float sum;
	int record_count;
	char min_employee_name[50];
	char max_employee_name[50];
	char min_employee_motto[100];
	char max_employee_motto[100];
};

// column[0]: last name
//		1 : first name
//		2 : telephone
//		3 : sales
//		4 : motto
//
int process_sales(void * data, int cnt, const char ** cv)
{
	struct process_sales_data *d=(struct process_sales_data*)data;

    printf("[%s] [%s] [%s] [%s] [%s]\n", cv[0], cv[1], cv[2], cv[3], cv[4]);
	float sales=atof(cv[3]);
	++d->record_count;
	d->sum += sales;
	if( sales > d->max){
		d->max = sales;
		snprintf(d->max_employee_name,49,"%s %s", cv[1], cv[0]);
		snprintf(d->max_employee_motto, 99, "%s", cv[4]);
	}
	if(sales < d->min){
		d->min = sales;
		snprintf(d->min_employee_name,49,"%s %s", cv[1], cv[0]);
		snprintf(d->min_employee_motto, 99, "%s", cv[4]);
	}				  

	return 0;
}

int main ()
{
	FILE *fp;
	struct process_sales_data d;

	d.min=1E12;
	d.max=0.f;
	d.sum=0.f;
	d.record_count=0;
	//if (argc != 2)
	//{
	//	printf("Usage: %s filename.csv\n", argv[0]);
	//	return 1;
	//}

	if ( NULL==(fp=fopen ("sales.csv","r") ) )
	{
		fprintf (stderr, "Cannot open input file sales.csv\n");
		return 2;
	}
	switch( csv_parse (fp, process_sales, &d) )
	{
	case E_LINE_TOO_WIDE:
		fprintf(stderr,"Error parsing csv: line too wide.\n");
		break;
	case E_QUOTED_STRING:
		fprintf(stderr,"Error parsing csv: ill-formatted quoted string.\n");
		break;
	}
	
	fclose (fp);

	printf("%s has the maximum sales at %8.2f, his/her motto is: %s\n",
		d.max_employee_name, d.max, d.max_employee_motto);
	printf("%s has the minimum sales at %8.2f, his/her motto is: %s\n",
		d.min_employee_name, d.min, d.min_employee_motto);
	printf("Total sales is %10.2f, number of salesman is %d, average sales is %8.2f\n",
		d.sum, d.record_count, d.sum/d.record_count);

	return 0;
}

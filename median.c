#include <postgres.h>
#include <fmgr.h>
#include <utils/builtins.h>
#include "catalog/pg_type_d.h"

#if PG_VERSION_NUM < 120000 || PG_VERSION_NUM >= 130000
#error "Unsupported PostgreSQL version. Use version 12."
#endif

#ifdef PG_MODULE_MAGIC
PG_MODULE_MAGIC;
#endif

PG_FUNCTION_INFO_V1(median_transfn);

/* The DS to store internal state 
 * which is a sorted list and number
 * of values
 */
typedef struct SortMemoryState {
	Datum vals;
	int num_vals;
}SortMemoryState;

/* Linked list for sorted values */
typedef struct SortVals {
	Datum val;
	struct SortVals *next;		
}SortVals;

/* Datatype specific routines for comparison of values*/
SortVals *int8_cmp(int64 newval, SortVals **new_val_node, SortVals *sort_vals_head);
SortVals *float4_cmp(float newval, SortVals **new_val_node, SortVals *sort_vals_head);
SortVals *int4_cmp(int32 newval, SortVals **new_val_node, SortVals *sort_vals_head);
SortVals *int2_cmp(int16 newval, SortVals **new_val_node, SortVals *sort_vals_head);
SortVals *float8_cmp(double newval, SortVals **new_val_node, SortVals *sort_vals_head);
SortVals *string_cmp(char *newval, SortVals **new_val_node, SortVals *sort_vals_head);

/*
 * Median state transfer function.
 *
 * This function is called for every value in the set that we are calculating
 * the median for. On first call, the aggregate state, if any, needs to be
 * initialized.
 */
Datum
median_transfn(PG_FUNCTION_ARGS)
{
    SortMemoryState *state;
    SortVals *sort_vals_head = NULL;
    SortVals *new_val_node;
    Oid argtype;
    int64 newval;
    double newval_num;
    char *newval_str;

    MemoryContext agg_context;
    
    if (!AggCheckCallContext(fcinfo, &agg_context))
        elog(ERROR, "median_transfn called in non-aggregate context");
    if (!PG_ARGISNULL(0))
    {
		state = (SortMemoryState *) PG_GETARG_POINTER(0);
    		sort_vals_head = (SortVals *)DatumGetPointer(state->vals);
    }
    /* Initialize the internal state */ 
    else
    {
		state = (SortMemoryState *) MemoryContextAlloc(agg_context, sizeof(SortMemoryState));
	        state->vals = PointerGetDatum(NULL);
		state->num_vals = 0;
    }
    if (!PG_ARGISNULL(1))
    {	
		/* First value*/
   		if (sort_vals_head == NULL)
    		{
    			sort_vals_head = (SortVals *) MemoryContextAlloc(agg_context, sizeof(SortVals));
    			sort_vals_head->next = NULL;
			sort_vals_head->val = PG_GETARG_DATUM(1);

			state->num_vals = 1;
			state->vals  = PointerGetDatum(sort_vals_head);
			PG_RETURN_POINTER(state);	
    		}
    		new_val_node = (SortVals *) MemoryContextAlloc(agg_context, sizeof(SortVals));
    		new_val_node->next = NULL;
    		new_val_node->val = PG_GETARG_DATUM(1);

		/*
		 * Call the routines to insert new value into the correct position in
		 * a sorted array
		 */
		argtype = get_fn_expr_argtype(fcinfo->flinfo, 1);
		switch (argtype)
		{
			case TIMESTAMPTZOID:
			case INT8OID:
				 newval = PG_GETARG_INT64(1);
				 sort_vals_head = int8_cmp(newval, &new_val_node, sort_vals_head);
				break;
			case INT4OID:
				 newval = PG_GETARG_INT32(1);
				 sort_vals_head = int4_cmp(newval, &new_val_node, sort_vals_head);
				 break;
			case INT2OID:
				 newval = PG_GETARG_INT16(1);
	 			 sort_vals_head = int2_cmp(newval, &new_val_node, sort_vals_head);
				break;
			case FLOAT4OID:
				newval_num = PG_GETARG_FLOAT4(1);	
	 			 sort_vals_head = float4_cmp(newval_num, &new_val_node, sort_vals_head);
				break;
			case FLOAT8OID:
				newval_num = PG_GETARG_FLOAT8(1);	
	 			 sort_vals_head = float8_cmp(newval_num, &new_val_node, sort_vals_head);
				break;
			case TEXTOID:
				newval_str = text_to_cstring(PG_GETARG_TEXT_PP(1));
	 			 sort_vals_head = string_cmp(newval_str, &new_val_node, sort_vals_head);
				break;
		}
    }
    /* We ignore the NULLs */
    else 
	PG_RETURN_POINTER(state); 
    /* Set the internal state for this iteration */
    state->vals  = PointerGetDatum(sort_vals_head);
    state->num_vals++;
    PG_RETURN_POINTER(state);
}

/*
 * This is called by transition function to insert an integer value in
 * a sorted list
 */
SortVals *
int8_cmp(int64 newval, SortVals **new_val_node, SortVals *sort_vals_head)
{
    SortVals *sort_vals = sort_vals_head;
    SortVals *sort_vals_prev = NULL; 
    int64 currval;
    int inserted = false; 

    while(sort_vals != NULL)
    {
	currval = DatumGetInt64(sort_vals->val);
        if (currval <= newval)
	{
		sort_vals_prev = sort_vals;
		sort_vals = sort_vals->next;
	}
	else
	{
		if (sort_vals == sort_vals_head)
		{
			(*new_val_node)->next = sort_vals_head;
			sort_vals_head = *new_val_node;
		}
		else
		{
			sort_vals_prev->next = *new_val_node;
			(*new_val_node)->next = sort_vals;	
		}
		inserted = true;
		break;
    	}
	if (!inserted && sort_vals == NULL)
		sort_vals_prev->next = *new_val_node;
    }
    return sort_vals_head;
}

/*
 * This is called by transition function to insert an 
 * 32-bit integer value in a sorted list
 */
SortVals *
int4_cmp(int32 newval, SortVals **new_val_node, SortVals *sort_vals_head)
{
    SortVals *sort_vals = sort_vals_head;
    SortVals *sort_vals_prev = NULL; 
    int32 currval;
    int inserted = false;		 	
    
    while(sort_vals != NULL)
    {
	currval = DatumGetInt32(sort_vals->val);
	/*
	 * loop until the current value is not greater
	 * than new value
	 */
        if (currval <= newval)
	{
		sort_vals_prev = sort_vals;
		sort_vals = sort_vals->next;
	}
	else
	{
		/* Insert in the first position */
		if (sort_vals == sort_vals_head)
		{
			(*new_val_node)->next = sort_vals_head;
			sort_vals_head = *new_val_node;
		}
		/* Insert in the middle */
		else
		{
			sort_vals_prev->next = *new_val_node;
			(*new_val_node)->next = sort_vals;	
		}
		inserted = true;
		break;
    	}
	/* Insert in the end */
	if (!inserted && sort_vals == NULL)
		sort_vals_prev->next = *new_val_node;
    }
    return sort_vals_head;
}

/*
 * This is called by transition function to insert a 
 * 16-bit integer value in a sorted list
 */
SortVals *
int2_cmp(int16 newval, SortVals **new_val_node, SortVals *sort_vals_head)
{
    SortVals *sort_vals = sort_vals_head;
    SortVals *sort_vals_prev = NULL; 
    int16 currval;
    int inserted = false;		 	

    while(sort_vals != NULL)
    {
	currval = DatumGetInt16(sort_vals->val);
        if (currval <= newval)
	{
		sort_vals_prev = sort_vals;
		sort_vals = sort_vals->next;
	}
	else
	{
		if (sort_vals == sort_vals_head)
		{
			(*new_val_node)->next = sort_vals_head;
			sort_vals_head = *new_val_node;
		}
		else
		{
			sort_vals_prev->next = *new_val_node;
			(*new_val_node)->next = sort_vals;	
		}
		inserted = true;
		break;
    	}
	if (!inserted && sort_vals == NULL)
		sort_vals_prev->next = *new_val_node;
    }
    return sort_vals_head;
}

/*
 * This is called by transition function to insert a 
 * double value in a sorted list
 */
SortVals *
float8_cmp(double newval, SortVals **new_val_node, SortVals *sort_vals_head)
{
    SortVals *sort_vals = sort_vals_head;
    SortVals *sort_vals_prev = NULL; 
    double currval;
  int inserted = false;		 	

    while(sort_vals != NULL)
    {
	currval = DatumGetFloat8(sort_vals->val);
        if (currval <= newval)
	{
		sort_vals_prev = sort_vals;
		sort_vals = sort_vals->next;
	}
	else
	{
		if (sort_vals == sort_vals_head)
		{
			(*new_val_node)->next = sort_vals_head;
			sort_vals_head = *new_val_node;
		}
		else
		{
			sort_vals_prev->next = *new_val_node;
			(*new_val_node)->next = sort_vals;	
		}
		inserted = true;
		break;
    	}
	if (!inserted && sort_vals == NULL)
		sort_vals_prev->next = *new_val_node;
    }
    return sort_vals_head;
}

/*
 * This is called by transition function to insert a 
 * float value in a sorted list.
 */
SortVals *
float4_cmp(float newval, SortVals **new_val_node, SortVals *sort_vals_head)
{
    SortVals *sort_vals = sort_vals_head;
    SortVals *sort_vals_prev = NULL; 
    float currval;
    int inserted = false;		 	

    while(sort_vals != NULL)
    {
	currval = DatumGetFloat4(sort_vals->val);
        if (currval <= newval)
	{
		sort_vals_prev = sort_vals;
		sort_vals = sort_vals->next;
	}
	else
	{
		if (sort_vals == sort_vals_head)
		{
			(*new_val_node)->next = sort_vals_head;
			sort_vals_head = *new_val_node;
		}
		else
		{
			sort_vals_prev->next = *new_val_node;
			(*new_val_node)->next = sort_vals;	
		}
		inserted = true;
		break;
    	}
	if (!inserted && sort_vals == NULL)
		sort_vals_prev->next = *new_val_node;
    }
    return sort_vals_head;
}

/*
 * This is called by transition function to insert a 
 * text value in a sorted list.
 */
SortVals *
string_cmp(char *newval, SortVals **new_val_node, SortVals *sort_vals_head)
{
    SortVals *sort_vals = sort_vals_head;
    SortVals *sort_vals_prev = NULL; 
    char *currval;
    int inserted = false;		 	

    while(sort_vals != NULL)
    {
	currval = text_to_cstring(DatumGetTextPP(sort_vals->val));
        if (strcmp(currval,newval) <= 0)
	{
		sort_vals_prev = sort_vals;
		sort_vals = sort_vals->next;
	}
	else
	{
		if (sort_vals == sort_vals_head)
		{
			(*new_val_node)->next = sort_vals_head;
			sort_vals_head = *new_val_node;
		}
		else
		{
			sort_vals_prev->next = *new_val_node;
			(*new_val_node)->next = sort_vals;	
		}
		inserted = true;
		break;
    	}
	if (!inserted && sort_vals == NULL)
		sort_vals_prev->next = *new_val_node;
    }
    return sort_vals_head;
}
PG_FUNCTION_INFO_V1(median_finalfn);


/*
 * Median final function.
 *
 * This function is called after all values in the median set has been
 * processed by the state transfer function. It should perform any necessary
 * post processing and clean up any temporary state.
 */
Datum
median_finalfn(PG_FUNCTION_ARGS)
{
    MemoryContext agg_context;
    SortMemoryState *state;
    SortVals *sort_vals;
    int median_index, i;

    if (!AggCheckCallContext(fcinfo, &agg_context))
        elog(ERROR, "median_finalfn called in non-aggregate context");
     
     state = PG_ARGISNULL(0) ? NULL : (SortMemoryState *) PG_GETARG_POINTER(0);
     if (state == NULL)
	PG_RETURN_NULL();
     median_index = (int)  (state->num_vals/2);

     elog(LOG, "median_index : %d", median_index);
     elog(LOG, "state_vals : %d", state->num_vals);
     sort_vals = (SortVals *)DatumGetPointer(state->vals);

     for(i = 0; i < median_index; i++)
     	sort_vals = sort_vals->next;

     PG_RETURN_DATUM(sort_vals->val);
}

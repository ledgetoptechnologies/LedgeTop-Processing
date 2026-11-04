"""Preflight signed frame budgets only; no source authority or transaction."""
MAX_TOTAL_WIRE=72*1024*1024

def validate_wire_quota(*,source_bytes,selected_bytes,evidence_page_bytes,file_page_bytes):
 values=(source_bytes,selected_bytes)
 if any(type(x) is not int or x<0 or x>12*1024*1024 for x in values):raise ValueError('invalid source stream budget')
 total=sum(values)
 for pages in (evidence_page_bytes,file_page_bytes):
  if pages is None:continue
  if type(pages) is not list or not 2<=len(pages)<=512 or any(type(n) is not int or n<1 or n>65535 for n in pages):raise ValueError('invalid stream budget')
  wire=sum(n+1 for n in pages)
  if wire>24*1024*1024:raise ValueError('stream budget exceeded')
  total+=wire
 if (evidence_page_bytes is None)!=(file_page_bytes is None) or total>MAX_TOTAL_WIRE:raise ValueError('total stream budget exceeded')
 return total

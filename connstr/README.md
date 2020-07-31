# Connection string for godror

More advanced configurations can be set with a connection string such as:
`user=user password=pass connectString="(DESCRIPTION=(CONNECT_TIMEOUT=3)(ADDRESS_LIST=(ADDRESS=(PROTOCOL=tcp)(HOST=hostname)(PORT=port)))(CONNECT_DATA=(SERVICE_NAME=sn)))"`
as `tnsping` returns.

A configuration like this is how you would add functionality such as load balancing across multiple servers. 
The portion described in parenthesis above can also be set in the `ConnectString` field of `ConnectionParams`.

TL;DR; the short form is `user=username password=password connectString="[//]host[:port][/service_name][:server][/instance_name]"`, the long form is
`connectString="(DESCRIPTION= (ADDRESS=(PROTOCOL=tcp)(HOST=host)(PORT=port)) (CONNECT_DATA= (SERVICE_NAME=service_name) (SERVER=server) (INSTANCE_NAME=instance_name)))"`.

For backward compatibility, you can provide _ANYTHING_ on the first line, and logfmt-ed parameters
on the second line. Or no logfmt-ed parameters at all.
So 
  
  * `scott@tcps://salesserver1:1521/sales.us.example.com?ssl_server_cert_dn="cn=sales,cn=Oracle Context Server,dc=us,dc=example,dc=com"&sdu=8128&connect_timeout=60
poolSessionTimeout=42s password=tiger`
  * `scott/tiger@salesserver1/sales.us.example.com`
  * `oracle://scott:tiger@salesserver1/sales.us.example.com&poolSessionTimeout=42s`
  * `scott@tcps://salesserver1:1521/sales.us.example.com?ssl_server_cert_dn="cn=sales,cn=Oracle Context Server,dc=us,dc=example,dc=com"&sdu=8128&connect_timeout=60
poolSessionTimeout=42s password=tiger`

works, too.

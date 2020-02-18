# UML diagrams in markdown for use with plantuml

@startuml
scale 0.75
package "S3-netcdf-python" {
    package "CFA" {
        package "Parsers" {
            CFA_Parser <|-- CFA_netCDFParser
            class CFA_Parser {
                void __init__()
                bool is_file(Dataset nc_dataset)
                void read(s3Dataset s3_dataset, string filename)
                void write(CFADataset cfa_dataset, s3Dataset s3_dataset)
            }
            class CFA_netCDFParser {
                void __init__()
                bool is_file(Dataset nc_dataset)
                void read(s3Dataset s3_dataset, string filename)
                void write(CFADataset cfa_dataset, s3Dataset s3_dataset)
            }
        }
        package "CFAClasses"{
            CFADataset *-- CFAGroup
            CFAGroup *-- CFAVariable
            CFAGroup *-- CFADimension
            CFAVariable *-- CFAPartition
            class CFADataset{
                string name
                string format
                string cfa_version
                dict<CFAGroup> cfa_groups
                dict<object> metadata
                void __init__(string name, dict cfa_groups, string format, string cfa_version, dict metadata)
                string __repr__()
                CFAGroup __getitem__(string grp_name)
                CFAGroup | string __getattr__(string name)
                void __setattr__(grp_name, value)
                CFAGroup createGroup(string grp_name, dict metadata)
                CFAGroup getGroup(string grp_name)
                bool renameGroup(string old_name, string new_name)
                string getName()
                list<string> getGroups()
                dict getMetadata()
                string getCFAVersion()
                string getFormat()
            }

            class CFAGroup{
                string grp_name
                CFADataset dataset
                dict<object> metadata
                dict<CFAVariable> cfa_vars
                dict<CFADimension> cfa_dims
                void __init__(string group_name, CFADataset dataset, dict<CFADimension> cfa_dims, dict<CFAVariable> cfa_vars, dict<object> metadata)
                string __repr__()
                CFAVariable | CFADimension | dict<object> __getitem__(string name)
                CFAVariable | CFADimension | dict<object> __getattr__(string name)
                CFAVariable createVariable(string var_name, dtype nc_dtype, list<string> dim_names, array<int> subarray_shape, int max_subarray_size, dict<object> metadata)
                CFAVariable getVariable(string var_name)
                list<string> getVariables()
                bool renameVariable(string old_name, string new_name)
                CFADimension createDimension(string dim_name, int dim_len, string axis_type, dict<object> metadata)
                CFADimension getDimension(string dim_name)
                list<string> getDimensions()
                bool renameDimension(string old_name, string new_name)
                dict<object> getMetadata()
                string getName()
                CFADataset getDataset()
            }
            class CFAVariable{
                string var_name
                CFAGroup group
                dtype nc_dtype
                dict<object> metadata
                string cf_role
                list<string> cfa_dimensions
                list<string> pmdimensions
                array<int> pmshape
                string base
                array<int> _shape
                object nc_partition_group
                array<int> subarray_shape
                void __init__(string var_name, dtype nc_dtype, CFAGroup group, string cf_role, list<string> cfa_dimensions, list<string> pmdimensions, array<int> pmshape, string base, dict<object> metadata, object nc_partition_group)
                string __repr__()
                list<object> __getitem__(object in_key)
                CFAGroup getGroup()
                string getName()
                dtype getType()
                dict<object> getMetadata()
                list<string> getDimensions()
                string getRole()
                array<int> shape()
                string getBaseFilename()
                array<int> getPartitionMatrixShape()
                list<string> getPartitionMatrixDimensions()
                CFAPartition getPartition(array<int> index)
                void writePartition(CFAPartition partition)
                void writeInitialPartitionInfo(string cfa_version, Dataset | Group nc_parent)
                void parse(dict<object> cfa_metadata)
                string dump()
                void load(dict<object> cfa_metadata, Dataset | Group nc_object)
            }
            class CFADimension{
                string dim_name
                int dim_len
                dict<object> metadata
                string axis_type
                dtype nc_dtype
                void __init__(string dim_name, int dim_len, string axis_type, dict<object> metadata)
                string __repr__()
                string dump()
                dtype getType()
                void setType(dtype type)
                string getName()
                int getLen()
                string getAxisType()
            }
            class CFAPartition{
                array<int> index
                array<int> location
                string ncvar
                string file
                string format
                array<int> shape
            }
        }
        package CFAExceptions{
            CFAError <|-- CFAGroupError
            CFAError <|-- CFADimensionError
            CFAError <|-- CFAVariableError
            CFAError <|-- CFAVariableIndexError
            CFAError <|-- CFAPartitionError
            CFAError <|-- CFAPartitionIndexError
            CFAError <|-- CFASubArrayError

            class CFAError{}

            class CFAGroupError{}

            class CFADimensionError{}

            class CFAVariableError{}

            class CFAVariableIndexError{}

            class CFAPartitionError{}

            class CFAPartitionIndexError{}

            class CFASubArrayError{}
        }
        class CFASplitter{
            array<int> shape
            array<float> sub
            list<string> axis_types
            int max_subarray_size

            void __init__(array<int> shape, int max_subarray_size, list<string> axis_types)
            array<int> calculateSubarrayShape()
            void setSubarrayShape(array<int> subarray_shape)
        }
    }

    package "Backends" {
        class s3aioFileObject{

        }
        class s3FileObject{

        }
    }

    package "Managers" {
        class ConfigManager{

        }

        class ConnectionPool{

        }

        class FileManager{

        }
    }

    class s3Dimension{

    }

    class s3Variable{
    }

    class s3Group{

    }

    class s3Dataset{

    }
}
@enduml

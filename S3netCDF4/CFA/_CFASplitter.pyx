"""
   CFASplitter class containing the routines required to take a
   multi-dimensional array and split it into subarrays according to the protocol
   that each subarray should have a maximum size, and that the number of
   operations required to read the entire array in any direction should be
   equal.

"""

__copyright__ = "(C) 2019 Science and Technology Facilities Council"
__license__ = "BSD - see LICENSE file in top-level directory"
__authors__ = "Neil Massey and Matthew Jones"

import numpy as np
cimport numpy as np

cdef class CFASplitter:
    """
       Class containing the methods required to return optimised subarrays for
       creating CFAVariables.
    """

    cdef np.ndarray shape
    cdef np.ndarray subarray_shape
    cdef list axis_types
    cdef int max_subarray_size


    def __init__(self,
                 np.ndarray shape,
                 int max_subarray_size=0,
                 list axis_types=[],
                ):
        """Initialise the CFA array splitter.

        Args:
            shape (np.ndarray): the shape of the array to split into subarrays.
            axis_types (list): a list of the types of axis, in order, for the
                shape of the array.  These axis types can be:
                    'X' - X axis
                    'Y' - Y axis
                    'Z' - Z / level axis
                    'T' - Time axis
                    'N' - non of the above axis
                    'U' - unspecified axis, this needs to be overwritten
        """
        DEFAULT_SUBARRAY_SIZE = 50*1024*1024 # 50MB default object size
        self.shape = shape
        if len(axis_types) == 0:
            # build the axis_types by guessing what they should be
            # this order follows CF conventions
            default_axis_types = ["T", "Z", "Y", "X"]
            new_axis_types = np.empty(shape.size)
            # position in default axis array
            p = len(default_axis_types)-1
            for i in range(shape.size, 0, -1):
                # calculate the default axis position
                if p >= 0:
                    new_axis_types[i] = default_axis_types[p]
                    # go to the next (previous) default axis type
                    p -= 1
                else:
                    new_axis_types[i] = 'N'
            self.axis_types = new_axis_types
        else:
            self.axis_types = axis_types

        if max_subarray_size == 0:
            self.max_subarray_size = DEFAULT_SUBARRAY_SIZE
        else:
            self.max_subarray_size = max_subarray_size

        self.subarray_shape = np.array([])


    cdef _numVals(self, np.ndarray shape):
        """Return number of values in subarray of specified shape, given by a
        list of dimension lengths.

        shape -- list of subarray dimension sizes"""
        if (len(shape) == 0):
            return 1
        return np.prod(shape)


    cdef _subdivideArray(self,
                          np.ndarray c_subarray_divs,
                          list permitted_axes=["T"]):
        # calculate the number of elements per sub for the linear axis types
        n_per_subf = np.empty((len(self.shape),),'i')
        for i in range(0, len(self.shape)):
            if self.axis_types[i] not in permitted_axes:
                n_per_subf[i] = int(1e6)
            # check that we are not going to subdivide more than the axis length!
            elif c_subarray_divs[i] >= self.shape[i]:
                n_per_subf[i] = int(1e6)
            else:
                n_per_subf[i] = c_subarray_divs[i]
        # get the minimum index
        min_i = np.argmin(n_per_subf)
        c_subarray_divs[min_i] += 1
        return c_subarray_divs


    cdef _getLinearOperations(self, np.ndarray c_subarray_divs):
        """Get the number of operations required to read one spatial point for
           every timestep through the dataset.
           This is equal to: number of subarrays in the T axis."""
        # get the t axis index, if it exists, otherwise the Z axis, otherwise
        # the N axis
        t_ax = -1
        if "T" in self.axis_types:
            t_ax = self.axis_types.index("T")
        elif "Z" in self.axis_types:
            t_ax = self.axis_types.index("Z")
        elif "N" in self.axis_types:
            t_ax = self.axis_types.index("N")

        # calculate number of operations
        if t_ax != -1:
            return c_subarray_divs[t_ax]
        else:
            # otherwise return -1
            return -1


    cdef _getFieldOperations(self, np.ndarray c_subarray_divs):
        """Get the number of operations required to read one 2D field of data at
           a particular timestep or level throughout the dataset.
           This is equal to: (subarrays in the X axis) *
                             (subarrays in the Y axis)
        """
        # get the X and Y axes, if they exists
        x_ax = -1
        y_ax = -1
        if "X" in self.axis_types:
            x_ax = self.axis_types.index("X")
        if "Y" in self.axis_types:
            y_ax = self.axis_types.index("Y")

        # four possibilities:
        # 1. X & Y exist            : return subarrays in X * subarrays in Y
        # 2. X exists but Y doesn't : return subarrays in X
        # 3. Y exists but X doesn't : return subarrays in Y
        # 4. Neither X or Y exists  : return -1

        # logic optimised
        if not (x_ax == -1 or y_ax == -1):
            n_ops = c_subarray_divs[x_ax] * c_subarray_divs[y_ax]
        elif y_ax != -1:
            n_ops = c_subarray_divs[y_ax]
        elif x_ax != -1:
            n_ops = c_subarray_divs[x_ax]
        else:
            n_ops = -1

        return n_ops


    cpdef calculateSubarrayShape(self):
        """
        Return a 'good shape' for the sub-arrays for an any-D variable,
        assuming balanced 1D/(n-1)D access

        Returns floating point field lengths of a field shape that provides
        balanced access of 1D subsets and 2D subsets of a netCDF or HDF5
        variable with any shape.
        'Good shape' for fields means that the number of fields accessed to read
        either kind of 1D or 2D subset is approximately equal, and the size of
        each field is no more than max_subarray_size.
        An extra complication here is that we wish to be able to optimise for any number of
        dimensions (1,2,3,4, etc.) but ensure that the algorithm knows which axis it is
        operating on.  For example, a 2D field with X and Y axes should not be split in
        the same way as a 2D field with T and Z axes.

        The algorithm follows a sub-division process, in this order (if they
        exist):
            1. sub divide the X axis
            2. sub divide the T axis
            3. sub divide the Y axis
            4. sub divide the Z axis
            5. sub divide any N axes

        Calculating the access operations:
            There are two "types" of access operations
             - linear (accessing a single spatial point across timesteps)
             - field  (accessing a 2D field of data at a particular timestep)
            The number of access operations are:
             - linear :  T dimension / number of subfields in the T axis
             - field  : (X dimension / number of subfields in the X axis)*
                        (Y dimension / number of subfields in the Y axis)
        """

        # the algorithm first calculates how many partitions each dimension
        # should be split into - this is stored in c_subfield_divs
        # current subfield_repeats shape defaults to var shape
        c_subarray_divs = np.ones((len(self.shape),), 'i')

        # if the number of values in the field_shape is greater than
        # max_subarray_size then divide
        while (self._numVals(self.shape / c_subarray_divs)) > self.max_subarray_size:
            # get the linear access and the field access operations
            linear_ops = self._getLinearOperations(c_subarray_divs)
            field_ops  = self._getFieldOperations(c_subarray_divs)
            # choose to divide on field ops first, if the number of ops are equal
            if field_ops <= linear_ops:
                c_subarray_divs = self._subdivideArray(c_subarray_divs,
                                                        ["X", "Y"]
                                                       )
            else:
                c_subarray_divs = self._subdivideArray(c_subarray_divs,
                                                        ["T", "Z", "N"]
                                                       )

        # we have so far calculated the optimum number of times each axis will
        # be divided
        # - translate this into a (floating point) number of elements in each
        #   chunk, for each axis
        c_subarray_shape = np.array(self.shape, 'd') / c_subarray_divs
        self.subarray_shape = c_subarray_shape
        return c_subarray_shape


    cpdef setSubarrayShape(self, np.ndarray subarray_shape):
        """Set the shape of the subarray, for when the user wishes to define it.
        """
        self.subarray_shape = subarray_shape
        return subarray_shape

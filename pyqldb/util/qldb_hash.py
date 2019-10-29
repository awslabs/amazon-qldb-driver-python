# Copyright 2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License"). You may not use this file except in compliance with
# the License. A copy of the License is located at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# or in the "license" file accompanying this file. This file is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
# CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions
# and limitations under the License.
from array import array
from hashlib import sha256

from amazon.ion.simpleion import loads, dumps
import ionhash

HASH_SIZE = 32


class QldbHash:
    """
    A QLDB hash is either a 256 bit number or a special empty hash.

    :raises ValueError: When the hash is None or is not the correct hash size.
    """
    def __init__(self, qldb_hash):
        if qldb_hash is None or (len(qldb_hash) != HASH_SIZE and len(qldb_hash) != 0):
            raise ValueError('Hash must either be empty or {} bytes long'.format(HASH_SIZE))
        self._qldb_hash = qldb_hash

    def __eq__(self, other):
        if not isinstance(other, QldbHash) or other is None:
            return False
        return QldbHash._hash_comparator(self.get_qldb_hash(), other.get_qldb_hash()) == 0

    def __repr__(self):
        return self._qldb_hash.hex()

    def dot(self, that):
        """
        Sort the current hash value and the hash value provided by `that`, comparing by their **signed** byte values in
        little-endian order.

        :type that: bytes
        :param that: The Ion hash of Ion value to compare.

        :rtype: :py:class:`pyqldb.util.qldb_hash.Qldbhash`/object
        :return: An QldbHash object that contains the concatenated hash values.
        """
        concatenated = QldbHash._join_hashes_pair_wise(self.get_qldb_hash(), that.get_qldb_hash())
        new_hash_lib = sha256()
        new_hash_lib.update(concatenated)
        new_digest = new_hash_lib.digest()
        return QldbHash(new_digest)

    def get_hash_size(self):
        return len(self._qldb_hash)

    def get_qldb_hash(self):
        return self._qldb_hash

    def is_empty(self):
        return len(self._qldb_hash) == 0

    @staticmethod
    def to_qldb_hash(value):
        """
        The QldbHash of an IonValue is just the IonHash of that value.

        :type value: str/:py:class:`amazon.ion.simple_types.IonSymbol`
        :param value: The string or Ion value to be converted to Ion hash.

        :rtype: :py:class:`pyqldb.util.qldb_hash.Qldbhash`/object
        :return: An QldbHash object that contains Ion hash.
        """
        if isinstance(value, str):
            value = loads(dumps(value))
        ion_hash = value.ion_hash('SHA256')
        ion_hash_digest = ion_hash
        return QldbHash(ion_hash_digest)

    @staticmethod
    def _hash_comparator(h1, h2):
        """
        Compares two hashes by their **signed** byte values in little-endian order.
        """
        h1_array = array('b', h1)
        h2_array = array('b', h2)

        if len(h1) != 32 or len(h2) != 32:
            raise ValueError("Invalid hash")
        for i in range(len(h1_array) - 1, -1, -1):
            difference = h1_array[i] - h2_array[i]
            if difference != 0:
                return difference
        return 0

    @staticmethod
    def _join_hashes_pair_wise(h1, h2):
        """
        Takes two hashes, sorts them, and concatenates them.
        """
        if len(h1) == 0:
            return h2
        if len(h2) == 0:
            return h1
        if QldbHash._hash_comparator(h1, h2) < 0:
            concatenated = h1 + h2
        else:
            concatenated = h2 + h1
        return concatenated

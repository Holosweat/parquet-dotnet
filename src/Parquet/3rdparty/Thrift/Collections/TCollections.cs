#pragma warning disable CS1591 // Missing XML comment for publicly visible type or member
#pragma warning disable IDE0008 // Use explicit type

// Licensed to the Apache Software Foundation(ASF) under one
// or more contributor license agreements.See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
// 
//     http://www.apache.org/licenses/LICENSE-2.0
// 
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

using System.Collections;
using System.Collections.Generic;

namespace Thrift.Collections {
    // ReSharper disable once InconsistentNaming
    public static class TCollections {
        /// <summary>
        ///     This will return true if the two collections are value-wise the same.
        ///     If the collection contains a collection, the collections will be compared using this method.
        /// </summary>
        public static bool Equals(IEnumerable first, IEnumerable second) {
            if(first == null && second == null) {
                return true;
            }

            if(first == null || second == null) {
                return false;
            }

            // for dictionaries, we need to compare keys and values separately
            // because KeyValuePair<K,V>.Equals() will not do what we want
            var fdict = first as IDictionary;
            var sdict = second as IDictionary;
            if((fdict != null) || (sdict != null)) {
                if((fdict == null) || (sdict == null))
                    return false;
                return TCollections.Equals(fdict.Keys, sdict.Keys)
                    && TCollections.Equals(fdict.Values, sdict.Values);
            }

            var fiter = first.GetEnumerator();
            var siter = second.GetEnumerator();

            var fnext = fiter.MoveNext();
            var snext = siter.MoveNext();

            while(fnext && snext) {
                var fenum = fiter.Current as IEnumerable;
                var senum = siter.Current as IEnumerable;

                if(fenum != null && senum != null) {
                    if(!Equals(fenum, senum)) {
                        return false;
                    }
                }
                else if(fenum == null ^ senum == null) {
                    return false;
                }
                else if(!Equals(fiter.Current, siter.Current)) {
                    return false;
                }

                fnext = fiter.MoveNext();
                snext = siter.MoveNext();
            }

            return fnext == snext;
        }

        /// <summary>
        ///     This returns a hashcode based on the value of the enumerable.
        /// </summary>
        public static int GetHashCode(IEnumerable enumerable) {
            if(enumerable == null) {
                return 0;
            }

            var hashcode = 0;

            foreach(var obj in enumerable) {
                var objHash = (obj is IEnumerable enum2) ? GetHashCode(enum2) : obj?.GetHashCode() ?? 0;

                unchecked {
                    hashcode = (hashcode * 397) ^ (objHash);
                }
            }

            return hashcode;
        }


    }
}

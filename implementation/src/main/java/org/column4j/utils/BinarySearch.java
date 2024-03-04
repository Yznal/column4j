package org.column4j.utils;

public class BinarySearch {

    private BinarySearch() {
    }


    public static int leftSearch(byte[] arr, int begin, int end, byte value) {
        if (arr[begin] == value) {
            return 0;
        }
        int l = begin, r = end;
        while (r - l > 1) {
            int mid = l + (r - l) / 2;
            if (arr[mid] < value) {
                l = mid;
            } else {
                r = mid;
            }
        }
        if (r >= arr.length || arr[r] != value) {
            return -r;
        }
        return r;
    }

    public static int leftSearch(short[] arr, int begin, int end, short value) {
        if (arr[begin] == value) {
            return 0;
        }
        int l = begin, r = end;
        while (r - l > 1) {
            int mid = l + (r - l) / 2;
            if (arr[mid] < value) {
                l = mid;
            } else {
                r = mid;
            }
        }
        if (r >= arr.length || arr[r] != value) {
            return -r;
        }
        return r;
    }

    public static int leftSearch(int[] arr, int begin, int end, int value) {
        if (arr[begin] == value) {
            return 0;
        }
        int l = begin, r = end;
        while (r - l > 1) {
            int mid = l + (r - l) / 2;
            if (arr[mid] < value) {
                l = mid;
            } else {
                r = mid;
            }
        }
        if (r >= arr.length || arr[r] != value) {
            return -r;
        }
        return r;
    }

    public static int leftSearch(long[] arr, int begin, int end, long value) {
        if (arr[begin] == value) {
            return 0;
        }
        int l = begin, r = end;
        while (r - l > 1) {
            int mid = l + (r - l) / 2;
            if (arr[mid] < value) {
                l = mid;
            } else {
                r = mid;
            }
        }
        if (r >= arr.length || arr[r] != value) {
            return -r;
        }
        return r;
    }

    public static int leftSearch(float[] arr, int begin, int end, float value) {
        if (arr[begin] == value) {
            return 0;
        }
        int l = begin, r = end;
        while (r - l > 1) {
            int mid = l + (r - l) / 2;
            if (arr[mid] < value) {
                l = mid;
            } else {
                r = mid;
            }
        }
        if (r >= arr.length || arr[r] != value) {
            return -r;
        }
        return r;
    }

    public static int leftSearch(double[] arr, int begin, int end, double value) {
        if (arr[begin] == value) {
            return 0;
        }
        int l = begin, r = end;
        while (r - l > 1) {
            int mid = l + (r - l) / 2;
            if (arr[mid] < value) {
                l = mid;
            } else {
                r = mid;
            }
        }
        if (r >= arr.length || arr[r] != value) {
            return -r;
        }
        return r;
    }


    public static int leftIndirectSearch(int[] pointers, byte[] arr, int begin, int end, byte value) {
        if (arr[pointers[begin]] == value) {
            return 0;
        }
        int l = begin, r = end;
        while (r - l > 1) {
            int mid = l + (r - l) / 2;
            if (arr[pointers[mid]] < value) {
                l = mid;
            } else {
                r = mid;
            }
        }
        if (r >= arr.length || arr[pointers[r]] != value) {
            return -r;
        }
        return r;
    }

    public static int leftIndirectSearch(int[] pointers, short[] arr, int begin, int end, short value) {
        if (arr[pointers[begin]] == value) {
            return 0;
        }
        int l = begin, r = end;
        while (r - l > 1) {
            int mid = l + (r - l) / 2;
            if (arr[pointers[mid]] < value) {
                l = mid;
            } else {
                r = mid;
            }
        }
        if (r >= arr.length || arr[pointers[r]] != value) {
            return -r;
        }
        return r;
    }

    public static int leftIndirectSearch(int[] pointers, int[] arr, int begin, int end, int value) {
        if (arr[pointers[begin]] == value) {
            return 0;
        }
        int l = begin, r = end;
        while (r - l > 1) {
            int mid = l + (r - l) / 2;
            if (arr[pointers[mid]] < value) {
                l = mid;
            } else {
                r = mid;
            }
        }
        if (r >= arr.length || arr[pointers[r]] != value) {
            return -r;
        }
        return r;
    }

    public static int leftIndirectSearch(int[] pointers, long[] arr, int begin, int end, long value) {
        if (arr[pointers[begin]] == value) {
            return 0;
        }
        int l = begin, r = end;
        while (r - l > 1) {
            int mid = l + (r - l) / 2;
            if (arr[pointers[mid]] < value) {
                l = mid;
            } else {
                r = mid;
            }
        }
        if (r >= arr.length || arr[pointers[r]] != value) {
            return -r;
        }
        return r;
    }

    public static int leftIndirectSearch(int[] pointers, float[] arr, int begin, int end, float value) {
        if (arr[pointers[begin]] == value) {
            return 0;
        }
        int l = begin, r = end;
        while (r - l > 1) {
            int mid = l + (r - l) / 2;
            if (arr[pointers[mid]] < value) {
                l = mid;
            } else {
                r = mid;
            }
        }
        if (r >= arr.length || arr[pointers[r]] != value) {
            return -r;
        }
        return r;
    }

    public static int leftIndirectSearch(int[] pointers, double[] arr, int begin, int end, double value) {
        if (arr[pointers[begin]] == value) {
            return 0;
        }
        int l = begin, r = end;
        while (r - l > 1) {
            int mid = l + (r - l) / 2;
            if (arr[pointers[mid]] < value) {
                l = mid;
            } else {
                r = mid;
            }
        }
        if (r >= arr.length || arr[pointers[r]] != value) {
            return -r;
        }
        return r;
    }


}

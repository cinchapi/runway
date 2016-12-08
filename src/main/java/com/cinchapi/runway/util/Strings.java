package com.cinchapi.runway.util;

/**
 * Yet another collection of string utilities.
 * 
 * @author jnelson
 */
public final class Strings {

    /**
     * Split a string on a delimiter as long as that delimiter is not wrapped in
     * double quotes.
     * 
     * @param string
     * @param delimiter
     * @return the split string tokens
     */
    public static String[] splitStringByDelimterAndRespectQuotes(String string,
            String delimiter) {
        // http://stackoverflow.com/a/15739087/1336833
        return string.split(delimiter + "(?=([^\"]*\"[^\"]*\")*[^\"]*$)");
    }

    private Strings() {/* noop */}

}

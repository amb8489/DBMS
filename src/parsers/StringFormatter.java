package parsers;

import java.util.ArrayList;
import java.util.List;

//
public class StringFormatter {


    public static String format(String stmt) {

        // step 1 remove all new line chars
        stmt = stmt.replace("\n", " ").strip();
        // step 2 remove any redundant spaces

        StringBuilder str = new StringBuilder();
        boolean insideQuoats = false;
        boolean SpaceAlreadySeen = false;

        for (int i = 0; i < stmt.length(); i++) {
            char ch = stmt.charAt(i);

            if (!insideQuoats) {

                if (ch == ' ') {

                    if (!SpaceAlreadySeen) {
                        str.append(ch);
                        SpaceAlreadySeen = true;
                    }

                } else {
                    str.append(ch);
                    if (ch == '\"') {
                        insideQuoats = true;
                    }
                    SpaceAlreadySeen = false;
                }
            }else {
                str.append(ch);
                if (ch == '\"') {
                    insideQuoats = false;
                }
            }

        }
        return str.toString();
    }


        public static void main (String[]args){
            String stmt = "     hi     \"Aaron    Berghash\"          is       here";
            System.out.println(format(stmt));
        }

    public static List<String> mkTokensFromStr(String stmt) {
        stmt = format(stmt);
        List<String> tokens = new ArrayList<>();

        StringBuilder tk = new StringBuilder();
        boolean inQuoats = false;
        for (int i = 0; i < stmt.length(); i++) {
            char ch = stmt.charAt(i);

            if (!inQuoats && ch == ' ') {

                if (!tk.toString().isBlank()) {
                    tokens.add(tk.toString());
                    tk = new StringBuilder();
                }
            } else {

                if (ch == '\"') {
                    inQuoats = !inQuoats;
                }
                tk.append(ch);
            }
        }
        if (tk.length() > 0) {
            tokens.add(tk.toString());
        }

        tokens.removeIf(String::isBlank);

        return tokens;
    }


}

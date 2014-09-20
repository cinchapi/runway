package org.cinchapi.runway.validation;

public class EmailValidator implements Validator {

    @Override
    public boolean validate(Object object) {
        if(object instanceof String){
            return ((String) object).contains("@");
        }
        else{
            return false;
        }
    }

    @Override
    public String getErrorMessage() {
        return "An email address must have one @ symbol";
    }

}

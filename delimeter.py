import os

# Get the absolute path for a file
file_path = os.path.abspath("mydata.csv")

# Print the file path
print(file_path)


function validateForm() {
  // Get the form element
  var form = document.getElementById("form1");

  // Get the value of the input field
  var inputValue = form.elements["delimiter"].value;

  // Check if the input value contains a delimiter
  if (inputValue.indexOf(",") === -1) {
    // If not, display an error message
    alert("Please enter a valid delimiter (e.g. ',')");
    return false;
  }

  // If the input value is valid, submit the form
  return true;
}



function validate() {
      // Get the input string
      const Delimiter = document.getElementById('Delimiter').value;

      // Validate the delimiters in the string
      const isValid = validateDelimiters(Delimiter);

      // Display a message indicating whether the delimiters are valid
      if (isValid) {
        alert('The delimiters in the string are valid.');
      } else {
        alert('The delimiters in the string are not valid.');
      }
    }

    function validateDelimiters(str) {
      # // Create a stack to store the delimiters
       const stack = [];

      # // Loop through the characters in the string
      # for (let i = 0; i < str.length; i++) {
      #   const char = str[i];
        const char=document.getElementById("Delimeter")
        // If the character is a left delimiter, push it onto the stack
        if (char === '(' || char === '{' || char === '['|| char ===',' || char===' ') {
          stack.push(char);
        }
        // If the character is a right delimiter, pop the top item off the stack
        else if (char === ')' || char === '}' || char === ']') {
          // If the top item on the stack is not the corresponding left delimiter, the delimiters are not balanced
          if (stack.pop() !== char) {
            return false;
          }
        }
      }

      // If the stack is empty, the delimiters are balanced
      return stack.length === 0;
    }
    


 function validate() {
      // Get the input string
      const input = document.getElementById('input').value;

      // Define the regular expression
      //const regex = /^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$/;

      // Test the input against the regular expression
      //const isValid = regex.test(input);

      // If the input is not valid, show an error message
      if (!isValid) {
        document.getElementById('error-message').innerHTML = 'Please enter a valid email address.';
        return false;
      }

      // If the input is valid, clear the error message
      document.getElementById('error-message').innerHTML = '';
      return true;
    }    
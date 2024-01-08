const stringProcessing = (str) => {
    // Split the string at each uppercase letter preceded by a lowercase letter
    const words = str.replace(/([a-z])([A-Z])/g, '$1 $2').split(' ');
  
    // Capitalize the first letter of each word and join them
    const readable = words.map(word => word.charAt(0).toUpperCase() + word.slice(1)).join(' ');
  
    return readable;
  };

  export default stringProcessing;